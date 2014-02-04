require 'net/ssh'
# Service that handles running external commands for Actions::Command
# Dynflow action. It runs just one (actor) thread for all the commands
# running in the system and updates the Dynflow actions periodically.
class SshConnector < Dynflow::MicroActor

  include Algebrick::Matching

  # command comming from action
  Command = Algebrick.type do
    fields(cmd: String,
           suspended_action: Dynflow::Action::Suspended)
  end

  # process running the action
  Process = Algebrick.type do
    fields(command:    Command,
           pid:        Integer,
           channel:    Net::SSH::Connection::Channel,
           out:        StringIO)
  end

  # periodic event refreshing the actions with current data
  RefreshActions = Algebrick.atom

  # event striggered when process is started to propagate send
  # the pid to the suspended action
  StartedAction = Algebrick.type do
    fields process:     Process
  end

  # event triggered when some process is finished, cauing the immediate update
  # of the action
  FinishedAction = Algebrick.type do
    fields process:     Process,
           exit_status: Integer
  end

  # event sent to the action with the update data
  ProcessUpdate = Algebrick.type do
    fields(pid:         Integer,
           lines:       String,
           exit_status: type { variants(NilClass, Integer) } )
  end

  # message causing waiting for more output
  Wait = Algebrick.atom

  def self.initialize
    @instance = self.new
  end

  def self.instance
    @instance
  end

  def initialize(logger = Rails.logger, *args)
    super
    @ssh = ssh_connection('pitr')
    @process_by_output = {}
    @process_buffer = {}
    @refresh_planned = false
  end

  def run_cmd(cmd, suspended_action)
    self << Command[cmd, suspended_action]
  end

  def on_message(message)
    match(message,
          on(~Command) do |command|
            initialize_command(command)
            plan_next_refresh
          end,
          on(StartedAction.(~any)) do |process|
            refresh_action(process)
          end,
          on(RefreshActions) do
            refresh_actions
            @refresh_planned = false
            plan_next_refresh
          end,
          on(FinishedAction.(~any, ~any)) do |process, exit_status|
            refresh_action(process, @process_buffer[process], exit_status)
          end,
          on(Wait) do
            wait
          end)

    if @mailbox.empty? && outputs.any?
      self << Wait
    end
  end

  def initialize_command(command)
    out = StringIO.new
    status = 0

    new_channel = @ssh.open_channel do |channel|
      channel.exec(command[:cmd]) do |ch, success|
        raise "FAILED: couldn't execute command (ssh.channel.exec)" unless success

        channel.on_data do |ch, data|
          out << "STDOUT: #{data}"
        end

        channel.on_extended_data do |ch, type, data|
          out << "STDERR: #{data}"
        end

        channel.on_request("exit-status") do |ch, data|
          status        = data.read_long
          final_success = status == 0
        end

        channel.on_request("exit-signal") do |ch, data|
          # TODO nevim co dela presne, asi by se melo ukoncovat spojeni
        end
      end
    end

    pid = new_channel.local_id
    process = Process[command, pid, new_channel, out]
    @process_by_output[out] = process
    @process_buffer[process] = ""
  end

  def clear_process(process)
    @process_by_output.delete(process[:out])
    @process_buffer.delete(process)
  end

  def refresh_action(process, lines = "", exit_status = nil)
    return unless @process_buffer.has_key?(process)
    @process_buffer[process] = ""

    process[:command][:suspended_action] << ProcessUpdate[process[:pid],
                                                          lines,
                                                          exit_status]
    if exit_status
      clear_process(process)
    end
  end

  def refresh_actions
    @process_by_output.values.each do |process|
      lines = @process_buffer[process]
      refresh_action(process, lines) unless lines.empty?
    end
  end

  def plan_next_refresh
    if outputs.any? && !@refresh_planned
      Dyntask.world.clock.ping(self, Time.now + refresh_interval, RefreshActions)
      @refresh_planned = true
    end
  end

  def refresh_interval
    1
  end

  def wait
    @ssh.loop
    ready_outputs = outputs.select { |out| out.length > 0 }

    return unless ready_outputs.present?

    ready_outputs.each do |ready_output|
      process = @process_by_output[ready_output]

      ready_output.rewind
      while (line = ready_output.gets)
        @process_buffer[process] << line
      end

      self << FinishedAction[process, 0]
    end

  end

  def outputs
    @process_by_output.values.map { |process| process[:out] }
  end

  private

  def ssh_connection(user, password = nil)
    @ssh_connections ||= begin
      Net::SSH.start('127.0.0.1', 'pitr', :password => 'a', :paranoid => false)
    end
  end

  def ssh_close
    @ssh_connections.close unless @ssh_connections.closed?
  end

end

