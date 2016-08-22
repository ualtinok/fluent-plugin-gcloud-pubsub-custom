require 'fluent/output'

require 'fluent/plugin/gcloud_pubsub/client'

module Fluent
  class GcloudPubSubOutput < BufferedOutput
    Fluent::Plugin.register_output('gcloud_pubsub', self)

    config_param :project,            :string,  :default => nil
    config_param :key,                :string,  :default => nil
    config_param :topic,              :string
    config_param :autocreate_topic,   :bool,    :default => false
    config_param :max_messages,       :integer, :default => 1000
    config_param :max_total_size,     :integer, :default => 10000000  # 10MB
    config_param :format,             :string,  :default => 'json'

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super
      @formatter = Plugin.new_formatter(@format)
      @formatter.configure(conf)
    end

    def start
      super
      @publisher = Fluent::GcloudPubSub::Publisher.new @project, @key, @topic, @autocreate_topic
      log.debug "connected topic:#{@topic} in project #{@project}"
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      messages = []
      size = 0

      chunk.msgpack_each do |tag, time, record|
        msg = @formatter.format(tag, time, record)
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

    private

    def publish(messages)
      log.debug "send message topic:#{@topic} length:#{messages.length.to_s}"
      @publisher.publish messages
    end
  end
end
