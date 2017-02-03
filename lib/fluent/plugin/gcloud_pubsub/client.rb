require 'google/cloud/pubsub'
require 'retryable'

module Fluent
  module GcloudPubSub
    class Error < StandardError
    end
    class RetryableError < Error
    end

    class Publisher
      RETRY_COUNT = 5
      RETRYABLE_ERRORS = [Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError]

      def initialize(project, key, topic_name, autocreate_topic)
        Retryable.retryable(tries: RETRY_COUNT, on: RETRYABLE_ERRORS) do
          pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key
          @client = pubsub.topic topic_name, autocreate: autocreate_topic
        end
        raise Error.new "topic:#{topic_name} does not exist." if @client.nil?
      end

      def publish(messages)
        @client.publish do |batch|
          messages.each do |m|
            batch.publish m
          end
        end
      rescue Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError => ex
        raise RetryableError.new "Google api returns error:#{ex.class.to_s} message:#{ex.to_s}"
      end
    end

    class Subscriber
      RETRY_COUNT = 5
      RETRYABLE_ERRORS = [Google::Cloud::UnavailableError, Google::Cloud::DeadlineExceededError, Google::Cloud::InternalError]

      def initialize(project, key, topic_name, subscription_name)
        Retryable.retryable(tries: RETRY_COUNT, on: RETRYABLE_ERRORS) do
          pubsub = Google::Cloud::Pubsub.new project: project, keyfile: key
          topic = pubsub.topic topic_name
          @client = topic.subscription subscription_name
        end
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
