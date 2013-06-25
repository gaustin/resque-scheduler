require 'resque/scheduler/lock/base'

module Resque
  class Scheduler
    module Lock
      class Resilient < Base
        def acquire!
          redis.evalsha(
            acquire_sha,
            :keys => [namespaced_key],
            :argv => [value]
          ).to_i == 1
        end

        def locked?
          redis.evalsha(
            locked_sha,
            :keys => [namespaced_key],
            :argv => [value]
          ).to_i == 1
        end

      private
        def redis
          Resque.backend.store
        end

        def namespace
          redis.namespace
        end

        def namespaced_key
          "#{namespace}:#{key}"
        end

        def locked_sha(refresh = false)
          @locked_sha = nil if refresh

          @locked_sha ||= begin
            redis.script(
              :load,
              <<-EOF
if redis.call('GET', KEYS[1]) == ARGV[1]
then
  redis.call('EXPIRE', KEYS[1], #{timeout})

  if redis.call('GET', KEYS[1]) == ARGV[1]
  then
    return 1
  end
end

return 0
EOF
            )
          end
        end

        def acquire_sha(refresh = false)
          @acquire_sha = nil if refresh

          @acquire_sha ||= begin
            redis.script(
              :load,
              <<-EOF
if redis.call('SETNX', KEYS[1], ARGV[1]) == 1
then
  redis.call('EXPIRE', KEYS[1], #{timeout})
  return 1
else
  return 0
end
EOF
            )
          end
        end
      end
    end
  end
end
