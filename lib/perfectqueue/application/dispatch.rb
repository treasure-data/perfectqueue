#
# PerfectQueue
#
# Copyright (C) 2012 FURUHASHI Sadayuki
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module PerfectQueue
  module Application

    class Dispatch < Runner
      # Runner interface
      def initialize(task)
        base = self.class.router.route(task.type)
        unless base
          task.retry!
          raise "Unknown task type #{task.type.inspect}"   # TODO error class
        end
        @runner = base.new(task)
        super
      end

      attr_reader :runner

      def run
        @runner.run
      end

      def kill(reason)
        @runner.kill(reason)
      end

      # DSL interface
      class << self
        def route(options)
          patterns = options.keys.select {|k| !k.is_a?(Symbol) }
          klasses = patterns.map {|k| options.delete(k) }
          patterns.zip(klasses).each {|pattern,sym|
            add_route(pattern, sym, options)
          }
          nil
        end

        def add_route(pattern, klass, options)
          router.add(pattern, klass, options)
        end

        def router=(router)
          (class<<self;self;end).instance_eval do
            self.__send__(:define_method, :router) { router }
          end
          router
        end

        def router
          self.router = Router.new
        end
      end
    end
  end
end

