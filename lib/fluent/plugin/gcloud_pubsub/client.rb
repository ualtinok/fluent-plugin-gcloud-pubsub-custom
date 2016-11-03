require 'google/cloud/pubsub'

module Fluent
  module GcloudPubSub
    class Error < StandardError
    end
    class RetryableError < Error
    end

    class Publisher
      def initialize(project, key, topic, autocreate_topic)
        pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key

        @client = pubsub.topic topic, autocreate: autocreate_topic
        raise Error.new "topic:#{topic} does not exist." if @client.nil?
      end

      def publish(messages)
        @client.publish do |batch|
          messages.each do |m|
            batch.publish m
          end
        end
      rescue Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError => ex
        raise RetryableError.new "Google api returns error:#{ex.class.to_s} message:#{ex.to_s}"
      end
    end

    class Subscriber
      def initialize(project, key, topic, subscription)
        pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key
        topic = pubsub.topic topic
        @client = topic.subscription subscription
        raise Error.new "subscription:#{subscription} does not exist." if @client.nil?
      end

      def pull(immediate, max)
        @client.pull immediate: immediate, max: max
      end

      def acknowledge(messages)
        @client.acknowledge messages
      end
    end
  end
end
