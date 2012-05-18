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

    class ApplicationRunner < Runner
      def initialize(base)
        @base = base
      end

      def run
        if before_run
          begin
            @base.run
          ensure
            after_run
          end
        end
      end

      def kill(reason)
        @base.kill(reason)
      end
    end

    class Base < Runner
      #def self.new(task)
      #  b = allocate
      #  b.task = task
      #  b.__send__(:initialize)
      #  ApplicationRunner.new(b)
      #end

      #def initialize
      #end

      def before_run
        true
      end

      def after_run
      end
    end

  end
end

