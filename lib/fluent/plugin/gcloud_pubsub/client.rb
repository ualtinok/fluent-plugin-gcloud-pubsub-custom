require 'google/cloud/pubsub'

module Fluent
  module GcloudPubSub
    class Error < StandardError
    end
    class RetryableError < Error
    end

    class Publisher
      def initialize(project, key, topic_name, autocreate_topic)
        pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key
        @client = pubsub.topic topic_name
        if @client.nil?
          if autocreate_topic
            @client = pubsub.create_topic topic_name
          else
            raise Error.new "topic:#{topic_name} does not exist."
          end
        end
      end

      def publish(messages)
        @client.publish do |batch|
          messages.each do |m|
            batch.publish m[0], uniq: m[1], ts: m[2]
          end
        end
      rescue Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError => ex
        raise RetryableError.new "Google api returns error:#{ex.class.to_s} message:#{ex.to_s}"
      end
    end

    class Subscriber
      def initialize(project, key, topic_name, subscription_name)
        pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key
        topic = pubsub.topic topic_name
        @client = topic.subscription subscription_name
        raise Error.new "subscription:#{subscription_name} does not exist." if @client.nil?
      end

      def pull(immediate, max)
        @client.pull immediate: immediate, max: max
      rescue Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError => ex
        raise RetryableError.new "Google pull api returns error:#{ex.class.to_s} message:#{ex.to_s}"
      end

      def acknowledge(messages)
        @client.acknowledge messages
      rescue Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError => ex
        raise RetryableError.new "Google acknowledge api returns error:#{ex.class.to_s} message:#{ex.to_s}"
      end
    end
  end
end
