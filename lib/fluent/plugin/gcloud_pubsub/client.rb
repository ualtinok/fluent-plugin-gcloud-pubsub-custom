require 'google/cloud'

module Fluent
  module GcloudPubSub
    class Error < StandardError; end

    class Publisher
      def initialize(project, key, topic, autocreate_topic)
        pubsub = (Google::Cloud.new project, key).pubsub

        @client = pubsub.topic topic, autocreate: autocreate_topic
        raise Fluent::GcloudPubSub::Error.new "topic:#{topic} does not exist." if @client.nil?
      end

      def publish(messages)
        @client.publish do |batch|
          messages.each do |m|
            batch.publish m
          end
        end
      end
    end

    class Subscriber
      def initialize(project, key, topic, subscription)
        pubsub = (Google::Cloud.new project, key).pubsub
        topic = pubsub.topic topic
        @client = topic.subscription subscription
        raise Fluent::GcloudPubSub::Error.new "subscription:#{subscription} does not exist." if @client.nil?
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
