#!/usr/bin/env ruby
# Run a CircleCI job from .circleci/config.yml locally inside Docker.
#
#   .circleci/run_job_locally.rb <job_name> [platform] [-p name=value ...]
#
#   platform: optional, e.g. linux/amd64, linux/arm64 (or just amd64/arm64).
#             When omitted, docker picks the default platform.
#   -p:       override a job parameter (e.g. -p test=true -p this_executor=noble_mysql84)
#   -n:       dry run, only print the expanded steps
#
# The primary executor image is started, secondary images (databases) share its
# network namespace like in CircleCI, then every step of the job (expanding
# reusable commands, parameters and when/unless) is executed in order. At the
# end you get an interactive shell inside the container; when you leave it all
# containers are removed.
#
# Workspaces (persist_to_workspace / attach_workspace) are shared between runs
# through the docker volume "circleci_local_workspace". Remove it with:
#   docker volume rm circleci_local_workspace

require 'yaml'
require 'shellwords'
require 'open3'

WORKSPACE_VOLUME = 'circleci_local_workspace'
WORKSPACE_DIR = '/tmp/circleci_local_workspace'
BASH_ENV_FILE = '/tmp/.circleci_local_bash_env'
$stdout.sync = true

def usage(msg = nil)
  warn "ERROR: #{msg}" if msg
  warn "Usage: #{$0} <job_name> [platform] [-n] [-p name=value ...]"
  exit 1
end

def info(msg)
  puts "\e[1;34m==> #{msg}\e[0m"
end

def warn_step(msg)
  puts "\e[1;33m==> #{msg}\e[0m"
end

# ---------------------------------------------------------------- arguments
job_name = nil
platform = nil
overrides = {}
dry_run = false
args = ARGV.dup
until args.empty?
  a = args.shift
  case a
  when '-h', '--help' then usage
  when '-n', '--dry-run' then dry_run = true
  when '-p', '--param'
    kv = args.shift or usage('-p needs name=value')
    k, v = kv.split('=', 2)
    usage("bad parameter '#{kv}'") if v.nil?
    overrides[k] = v
  else
    if job_name.nil? then job_name = a
    elsif platform.nil? then platform = a
    else usage("unexpected argument '#{a}'")
    end
  end
end
usage('missing job name') unless job_name
platform = "linux/#{platform}" if platform && !platform.include?('/')

repo_root = File.expand_path('..', __dir__)
config_file = File.join(repo_root, '.circleci', 'config.yml')
config = YAML.load_file(config_file)
jobs = config['jobs'] || {}
commands = config['commands'] || {}
executors = config['executors'] || {}

job = jobs[job_name] or usage("job '#{job_name}' not found in #{config_file}")

# ---------------------------------------------------------------- parameters
def param_defaults(defs)
  (defs || {}).each_with_object({}) { |(k, d), h| h[k] = d.is_a?(Hash) ? d['default'] : nil }
end

def cast(value, defn)
  return value unless value.is_a?(String) && defn.is_a?(Hash)
  case defn['type']
  when 'boolean' then %w[true yes on 1].include?(value.downcase)
  when 'integer' then value.to_i
  else value
  end
end

pipeline_params = param_defaults(config['parameters'])
git_branch = `git -C #{repo_root.shellescape} rev-parse --abbrev-ref HEAD 2>/dev/null`.strip
git_sha = `git -C #{repo_root.shellescape} rev-parse HEAD 2>/dev/null`.strip

TEMPLATE = /<<\s*([\w.-]+)\s*>>/

# Replaces << parameters.x >> / << pipeline.* >> in every string of obj.
# A string that is exactly one template keeps the parameter's native type.
subst = lambda do |obj, params|
  lookup = lambda do |key|
    case key
    when /\Aparameters\.(.+)\z/ then params.key?($1) ? params[$1] : (warn_step("unknown parameter #{key}"); '')
    when /\Apipeline\.parameters\.(.+)\z/ then pipeline_params[$1]
    when 'pipeline.git.branch' then git_branch
    when 'pipeline.git.revision' then git_sha
    when 'pipeline.number', 'pipeline.id' then '0'
    else "<< #{key} >>"
    end
  end
  case obj
  when String
    if (m = obj.strip.match(/\A<<\s*([\w.-]+)\s*>>\z/))
      lookup.call(m[1])
    else
      obj.gsub(TEMPLATE) { lookup.call($1).to_s }
    end
  when Array then obj.flat_map { |e| r = subst.call(e, params); e.is_a?(String) && r.is_a?(Array) ? r : [r] }
  when Hash then obj.each_with_object({}) { |(k, v), h| h[k] = subst.call(v, params) }
  else obj
  end
end

truthy = lambda do |cond|
  case cond
  when nil, false, '', 0, 'false' then false
  when Hash
    if cond.key?('equal') then cond['equal'].map(&:to_s).uniq.size == 1
    elsif cond.key?('not') then !truthy.call(cond['not'])
    elsif cond.key?('and') then cond['and'].all? { |c| truthy.call(c) }
    elsif cond.key?('or') then cond['or'].any? { |c| truthy.call(c) }
    else true
    end
  else true
  end
end

job_param_defs = job['parameters'] || {}
job_params = param_defaults(job_param_defs)
overrides.each do |k, v|
  warn_step("parameter '#{k}' is not declared by job #{job_name}") unless job_param_defs.key?(k)
  job_params[k] = cast(v, job_param_defs[k])
end

# ---------------------------------------------------------------- executor
docker_cfg = nil
working_dir = nil
job_s = subst.call(job.reject { |k, _| k == 'steps' }, job_params)
if job_s['docker']
  docker_cfg = job_s['docker']
  working_dir = job_s['working_directory']
else
  ex = job_s['executor'] or usage("job #{job_name} has neither docker nor executor")
  ex_name, ex_args = ex.is_a?(Hash) ? [ex['name'], ex.reject { |k, _| k == 'name' }] : [ex, {}]
  ex_def = executors[ex_name] or usage("executor '#{ex_name}' not found")
  ex_params = param_defaults(ex_def['parameters']).merge(ex_args)
  ex_s = subst.call(ex_def, ex_params)
  docker_cfg = ex_s['docker'] or usage("executor '#{ex_name}' is not a docker executor")
  working_dir = job_s['working_directory'] || ex_s['working_directory']
end
working_dir ||= '~/project'

stringify_env = ->(env) { (env || {}).each_with_object({}) { |(k, v), h| h[k.to_s] = v.nil? ? '' : v.to_s } }

primary = docker_cfg.first
job_env = stringify_env.call(primary['environment']).merge(stringify_env.call(job_s['environment']))
job_env.merge!(
  'CI' => 'true', 'CIRCLECI' => 'true', 'CIRCLE_JOB' => job_name,
  'CIRCLE_BRANCH' => git_branch, 'CIRCLE_SHA1' => git_sha, 'BASH_ENV' => BASH_ENV_FILE
)

# ---------------------------------------------------------------- steps
# Flattens a list of steps into [type, args] pairs, expanding commands.
expand = lambda do |steps, params, out|
  Array(steps).each do |raw|
    step = subst.call(raw, params)
    name, args = step.is_a?(Hash) ? step.first : [step, {}]
    args ||= {}
    case name
    when 'when', 'unless'
      cond = truthy.call(args['condition'])
      expand.call(args['steps'], params, out) if (name == 'when') == cond
    when 'run', 'checkout', 'persist_to_workspace', 'attach_workspace'
      out << [name, args]
    else
      if (cmd = commands[name])
        cmd_params = param_defaults(cmd['parameters'])
        args.each { |k, v| cmd_params[k] = v } if args.is_a?(Hash)
        expand.call(cmd['steps'], cmd_params, out)
      else
        out << ['skip', name]
      end
    end
  end
  out
end

steps = expand.call(job['steps'], job_params, [])

if dry_run
  info "Executor image(s): #{docker_cfg.map { |d| d['image'] }.join(', ')}  working_directory: #{working_dir}"
  steps.each_with_index do |(type, args), idx|
    text = type == 'run' ? (args.is_a?(String) ? args : args['command']).to_s.rstrip : "#{type} #{args.inspect}"
    puts "[#{idx + 1}/#{steps.size}] #{text.gsub("\n", "\n    ")}"
  end
  exit 0
end

# ---------------------------------------------------------------- docker helpers
container = "circleci-local-#{job_name.gsub(/[^\w.-]/, '_')}-#{Process.pid}"
containers = []
platform_args = platform ? ['--platform', platform] : []

cleanup = lambda do
  return if containers.empty?
  info "Removing containers: #{containers.join(' ')}"
  system('docker', 'rm', '-f', *containers, out: File::NULL, err: File::NULL)
  containers.clear
end
at_exit { cleanup.call }

def env_args(env)
  env.flat_map { |k, v| ['-e', "#{k}=#{v}"] }
end

def docker_capture(*cmd)
  out, status = Open3.capture2('docker', *cmd)
  [out.strip, status.success?]
end

info "Job: #{job_name}#{platform ? " (platform #{platform})" : ''}"
info "Parameters: #{job_params.map { |k, v| "#{k}=#{v.inspect}" }.join(', ')}" unless job_params.empty?

run_cmd = ['docker', 'run', '-d', '--name', container, *platform_args,
           '-v', "#{WORKSPACE_VOLUME}:#{WORKSPACE_DIR}", *env_args(job_env),
           '--entrypoint', 'tail', primary['image'], '-f', '/dev/null']
info "Starting #{primary['image']}"
system(*run_cmd, out: File::NULL) or abort('could not start primary container')
containers << container

docker_cfg.drop(1).each_with_index do |svc, i|
  name = "#{container}-svc#{i + 1}"
  cmd = ['docker', 'run', '-d', '--name', name, *platform_args, '--network', "container:#{container}",
         *env_args(stringify_env.call(svc['environment']))]
  cmd += ['--entrypoint', Array(svc['entrypoint']).first] if svc['entrypoint']
  cmd << svc['image']
  cmd += Array(svc['entrypoint']).drop(1) if svc['entrypoint']
  cmd += Array(svc['command']).map(&:to_s) if svc['command']
  info "Starting service #{svc['image']}"
  system(*cmd, out: File::NULL) or abort("could not start service #{svc['image']}")
  containers << name
end

home, = docker_capture('exec', container, 'sh', '-c', 'echo $HOME')
expand_home = ->(p) { p.to_s.sub(/\A~(?=\/|\z)/, home) }
working_dir = expand_home.call(working_dir)
uid, = docker_capture('exec', container, 'id', '-u')
gid, = docker_capture('exec', container, 'id', '-g')

as_root = lambda do |script|
  system('docker', 'exec', '-u', '0', container, 'sh', '-c', script)
end
as_root.call("mkdir -p #{working_dir.shellescape} #{WORKSPACE_DIR} && touch #{BASH_ENV_FILE} && " \
             "chown #{uid}:#{gid} #{working_dir.shellescape} #{WORKSPACE_DIR} #{BASH_ENV_FILE}")

# ---------------------------------------------------------------- step runners
exec_bash = lambda do |command, dir:, env: {}, shell: nil, background: false|
  shell_argv = shell ? Shellwords.split(shell) : ['/bin/bash', '-eo', 'pipefail']
  cmd = ['docker', 'exec', '-i', '-w', expand_home.call(dir), *env_args(env)]
  cmd << '-d' if background
  cmd += [container, *shell_argv, '-c', command]
  system(*cmd)
end

do_checkout = lambda do |path, label|
  dest = expand_home.call(path || working_dir)
  info "#{label} checkout -> #{dest}"
  files, = Open3.capture2('git', '-C', repo_root, 'ls-files', '-z', '-co', '--exclude-standard')
  list = files.split("\0").select { |f| File.exist?(File.join(repo_root, f)) } + ['.git']
  tar = ['tar', '-C', repo_root, '-cf', '-', '--null', '-T', '-']
  tar.insert(1, '--no-xattrs', '--no-mac-metadata') if RUBY_PLATFORM.include?('darwin')
  ok = false
  Open3.pipeline_w(tar, ['docker', 'exec', '-i', '-u', '0', container, 'tar', '-C', dest, '-xf', '-'],
                   err: File::NULL) do |stdin, threads|
    stdin.write(list.join("\0"))
    stdin.close
    ok = threads.all? { |t| t.value.success? }
  end
  ok && as_root.call("mkdir -p #{dest.shellescape} && chown -R #{uid}:#{gid} #{dest.shellescape}")
end


failed = false
begin
  steps.each_with_index do |(type, args), idx|
    label = "[#{idx + 1}/#{steps.size}]"
    if type == 'run'
      args = { 'command' => args } if args.is_a?(String)
      step_when = (args['when'] || 'on_success').to_s
      next if failed && step_when == 'on_success'
      next if !failed && step_when == 'on_fail'
      command = args['command'].to_s
      info "#{label} #{args['name'] || command.lines.first.to_s.strip}"
      ok = exec_bash.call(command, dir: args['working_directory'] ? File.expand_path(expand_home.call(args['working_directory']), working_dir) : working_dir,
                                   env: stringify_env.call(args['environment']), shell: args['shell'],
                                   background: args['background'])
      unless ok
        warn_step("#{label} step failed")
        failed = true
      end
    elsif failed
      next
    else
      case type
      when 'checkout'
        do_checkout.call(args.is_a?(Hash) ? args['path'] : nil, label) or (warn_step("#{label} checkout failed"); failed = true)
      when 'persist_to_workspace'
        root = File.expand_path(expand_home.call(args['root']), working_dir)
        paths = Array(args['paths']).map(&:shellescape).join(' ')
        info "#{label} persist_to_workspace #{root}: #{args['paths'].inspect}"
        exec_bash.call("tar -cf - #{paths} | tar -C #{WORKSPACE_DIR} -xf -", dir: root) or (failed = true)
      when 'attach_workspace'
        at = File.expand_path(expand_home.call(args['at']), working_dir)
        info "#{label} attach_workspace -> #{at}"
        as_root.call("mkdir -p #{at.shellescape} && cp -a #{WORKSPACE_DIR}/. #{at.shellescape}/ && chown -R #{uid}:#{gid} #{at.shellescape}") or (failed = true)
      when 'skip'
        warn_step("#{label} skipping unsupported step '#{args}'")
      end
    end
  end
rescue Interrupt
  warn_step('Interrupted')
  failed = true
end

if failed
  warn_step('Job FAILED. Dropping into the container so you can inspect it.')
else
  info 'Job finished successfully.'
end
if $stdin.tty?
  info "Interactive shell in #{container} (exit to remove the containers)"
  trap('INT', 'IGNORE')
  system('docker', 'exec', '-it', '-w', working_dir, container, 'bash', '-l') ||
    system('docker', 'exec', '-it', '-w', working_dir, container, 'sh')
  trap('INT', 'DEFAULT')
else
  warn_step('stdin is not a terminal, skipping the interactive shell')
end
cleanup.call
exit(failed ? 1 : 0)
