require 'gcloud'
require 'fluent/output'
require 'yajl'

module Fluent
  class GcloudPubSubOutput < BufferedOutput
    Fluent::Plugin.register_output('gcloud_pubsub', self)

    config_param :project,            :string,  :default => nil
    config_param :topic,              :string,  :default => nil
    config_param :key,                :string,  :default => nil
    config_param :autocreate_topic,   :bool,    :default => false
    config_param :max_messages,       :integer, :default => 1000
    config_param :max_total_size,     :integer, :default => 10000000  # 10MB

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super

      raise Fluent::ConfigError, "'topic' must be specified." unless @topic
    end

    def start
      super

      pubsub = (Gcloud.new @project, @key).pubsub
      @client = pubsub.topic @topic, autocreate: @autocreate_topic
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      messages = []
      size = 0

      chunk.msgpack_each do |tag, time, record|
        msg = Yajl.dump(record)
        if messages.length + 1 > @max_messages || size + msg.bytesize > @max_total_size
          publish messages
          messages = []
          size = 0
        end
        messages << msg
        size += msg.bytesize
      end

      if messages.length > 0
        publish messages
      end
    rescue => e
      log.error "unexpected error", :error=>$!.to_s
      log.error_backtrace
      raise e
    end

    def publish(messages)
      log.debug "send message topic:#{@client.name} length:#{messages.length.to_s}"
      @client.publish do |batch|
        messages.each do |m|
          batch.publish m
        end
      end
    end
  end
end
