require 'resque/scheduler/lock/base'

module Resque
  class Scheduler
    module Lock
      class Basic < Base
        def acquire!
          if redis.setnx(key, value)
            extend_lock!
            true
          end
        end

        def locked?
          if redis.get(key) == value
            extend_lock!

            if redis.get(key) == value
              return true
            end
          end

          false
        end

      private
        def redis
          Resque.backend.store
        end
      end
    end
  end
end
