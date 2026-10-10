# Shared process and receipt operations for finite native diagnostics.
require 'json'
require 'digest'
require 'fileutils'
require 'open3'
require 'optparse'
require 'tempfile'

module DiagnosticSupport
  def check(condition, message)
    raise message unless condition
  end

  def json(path)
    JSON.parse(File.read(path))
  end

  def save(path, value)
    Tempfile.create(['.receipt-', '.tmp'], File.dirname(path)) do |file|
      file.write(JSON.pretty_generate(value) + "\n")
      file.close
      File.rename(file.path, path)
    end
  end

  def digest(path)
    Digest::SHA256.file(path).hexdigest
  end

  def checked(command, cwd: nil)
    options = cwd ? { chdir: cwd } : {}
    output, status = Open3.capture2(*command, **options)
    check(status.success?, "command failed: #{command.inspect}")
    output.strip
  end

  def clock
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def wait_bounded(pid, seconds)
    deadline = clock + seconds
    loop do
      result = Process.waitpid2(pid, Process::WNOHANG)
      return result.last if result
      raise "fixed #{seconds}-second process bound exceeded" if clock >= deadline
      sleep 0.02
    end
  end

  def terminate_group(pid, privileged: false)
    %w[TERM KILL].each do |signal|
      if privileged
        system('sudo', '-n', 'kill', "-#{signal}", '--', "-#{pid}", out: File::NULL, err: File::NULL)
      else
        begin
          Process.kill(signal, -pid)
        rescue Errno::ESRCH
          # The child may exit between the timeout check and the kill. Still
          # wait below so this parent reaps it.
        end
      end
      begin
        return wait_bounded(pid, 5)
      rescue RuntimeError
        next
      end
    end
    Process.waitpid2(pid).last
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def run_process(command, out:, err:, timeout: 300, env: {}, cwd: nil, privileged: false)
    options = { out: out, err: err, pgroup: true }
    options[:chdir] = cwd if cwd
    pid = Process.spawn(env, *command, **options)
    begin
      status = wait_bounded(pid, timeout)
      { 'exit_code' => status.exitstatus || -status.termsig, 'error' => nil }
    rescue RuntimeError => error
      status = terminate_group(pid, privileged: privileged)
      { 'exit_code' => status && (status.exitstatus || -status.termsig), 'error' => error.message }
    end
  end
end
