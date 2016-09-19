require 'fluent/input'
require 'fluent/parser'

require 'fluent/plugin/gcloud_pubsub/client'

module Fluent
  class GcloudPubSubInput < Input
    Fluent::Plugin.register_input('gcloud_pubsub', self)

    config_param :tag,                :string
    config_param :project,            :string,  :default => nil
    config_param :key,                :string,  :default => nil
    config_param :topic,              :string
    config_param :subscription,       :string
    config_param :pull_interval,      :float,   :default => 5.0
    config_param :max_messages,       :integer, :default => 100
    config_param :return_immediately, :bool,    :default => true
    config_param :format,             :string,  :default => 'json'

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def configure(conf)
      super
      @parser = Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    def start
      super
      @subscriber = Fluent::GcloudPubSub::Subscriber.new @project, @key, @topic, @subscription
      log.debug "connected subscription:#{@subscription} in project #{@project}"
      @stop_subscribing = false
      @subscribe_thread = Thread.new(&method(:subscribe))
    end

    def shutdown
      super
      @stop_subscribing = true
      @subscribe_thread.join
    end

    private

    def subscribe
      until @stop_subscribing
        _subscribe

        if @return_immediately
          sleep @pull_interval
        end
      end
    rescue => ex
      log.error "unexpected error", error_message: ex.to_s, error_class: ex.class.to_s
      log.error_backtrace ex.backtrace
    end

    def _subscribe
      messages = @subscriber.pull @return_immediately, @max_messages
      if messages.length == 0
        log.debug "no messages are pulled"
        return
      end

      es = parse_messages(messages)
      if es.empty?
        log.warn "#{messages.length} message(s) are pulled, but no messages are parsed"
        return
      end

      begin
        router.emit_stream(@tag, es)
      rescue
        # ignore errors. Engine shows logs and backtraces.
      end
      @subscriber.acknowledge messages
      log.debug "#{messages.length} message(s) processed"
    rescue => ex
      log.error "unexpected error", error_message: ex.to_s, error_class: ex.class.to_s
      log.error_backtrace ex.backtrace
    end

    def parse_messages(messages)
      es = MultiEventStream.new
      messages.each do |m|
        convert_line_to_event(m.message.data, es)
      end
      es
    end

    def convert_line_to_event(line, es)
      line = line.chomp  # remove \n
      @parser.parse(line) { |time, record|
        if time && record
          es.add(time, record)
        else
          log.warn "pattern not match: #{line.inspect}"
        end
      }
    rescue => ex
      log.warn line.dump, error_message: ex.to_s, error_class: ex.class.to_s
      log.warn_backtrace ex.backtrace
    end
  end
end
