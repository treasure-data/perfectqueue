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

    class UndefinedDecisionError
    end

    class Decider
      def initialize(base)
        @base = base
      end

      def queue
        @base.queue
      end

      def task
        @base.task
      end

      def decide!(type, opts={})
        begin
          m = method(type)
        rescue NameError
          raise UndefinedDecisionError, "Undefined decision #{type} options=#{opt.inspect}"
        end
        m.call(type, opts)
      end
    end

    class DefaultDecider < Decider
      # no decisions defined
    end

  end
end

