#
# PerfectQueue
#
# Copyright (C) 2012-2013 Sadayuki Furuhashi
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

    class Base < Runner
      def self.decider
        DefaultDecider
      end

      def self.decider=(decider_klass)
        (class<<self;self;end).instance_eval do
          self.__send__(:define_method, :decider) { decider_klass }
        end
        decider_klass
      end

      def initialize(task)
        super
        @decider = self.class.decider.new(self)
      end

      attr_reader :decider

      def run
        begin
          return unless before_perform
          begin
            perform
          ensure
            after_perform
          end
        rescue
          decide! :unexpected_error_raised, :error=>$!
        end
      end

      def before_perform
        true
      end

      #def perform
      #end

      def after_perform
      end

      def decide!(type, option={})
        @decider.decide!(type, option)
      end
    end

  end
end

