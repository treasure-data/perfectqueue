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

    class Router
      def initialize
        @patterns = []
        @cache = {}
      end

      def add(pattern, sym, options={})
        case pattern
        when Regexp
          # ok
        when String, Symbol
          pattern = /#{Regexp.escape(pattern)}/
        else
          raise ArguementError, "pattern should be String or Regexp but got #{pattern.class}: #{pattern.inspect}"
        end

        @patterns << [pattern, sym]
      end

      def route(type)
        if @cache.has_key?(type)
          return @cache[type]
        end

        @patterns.each {|(pattern,sym)|
          if pattern.match(type)
            runner = resolve_application_base(sym)
            return @cache[type] = runner
          end
        }
        return @cache[type] = nil
      end

      private
      def resolve_application_base(klass)
        case klass
        when Symbol
          self.class.const_get(klass)
        else
          klass
        end
      end
    end

    class Dispatch
      # Runner interface
      def self.new(task)
        runner = router.route(task.type)
        unless runner
          task.release!
          raise "unknown task type #{task.type.inspect}"   # TODO error class
        end
        b = runner.new(task)
        return b
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

        private
        def router=(router)
          remove_method(:router) if method_defined?(:router)
          define_method(:router) { router }
        end

        def router
          router = Router.new
        end
      end
    end
  end
end

