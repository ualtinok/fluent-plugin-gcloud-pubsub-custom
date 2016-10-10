require 'json'
require 'webrick'

require 'fluent/input'
require 'fluent/parser'

require 'fluent/plugin/gcloud_pubsub/client'

module Fluent
  class GcloudPubSubInput < Input
    Fluent::Plugin.register_input('gcloud_pubsub', self)

    config_param :tag,                :string
    config_param :project,            :string,  default: nil
    config_param :key,                :string,  default: nil
    config_param :topic,              :string
    config_param :subscription,       :string
    config_param :pull_interval,      :float,   default: 5.0
    config_param :max_messages,       :integer, default: 100
    config_param :return_immediately, :bool,    default: true
    config_param :format,             :string,  default: 'json'
    # for HTTP RPC
    config_param :enable_rpc,         :bool,    default: false
    config_param :rpc_bind,               :string,  default: '0.0.0.0'
    config_param :rpc_port,               :integer, default: 24680

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    class RPCServlet < WEBrick::HTTPServlet::AbstractServlet
      class Error < StandardError; end

      def initialize(server, plugin)
        super
        @plugin = plugin
      end

      def do_GET(req, res)
        begin
          code, header, body = process(req, res)
        rescue
          code, header, body = render_json(500, {
              'ok' => false,
              'message' => 'Internal Server Error',
              'error' => "#{$!}",
              'backtrace'=> $!.backtrace
          })
        end

        res.status = code
        header.each_pair {|k,v|
          res[k] = v
        }
        res.body = body
      end

      def render_json(code, obj)
        [code, {'Content-Type' => 'application/json'}, obj.to_json]
      end

      def process(req, res)
        case req.path_info
        when '/stop'
          @plugin.stop_pull
        when '/start'
          @plugin.start_pull
        else
          raise Error.new "Invalid path_info: #{req.path_info}"
        end
        render_json(200, {'ok' => true})
      end
    end

    def configure(conf)
      super
      @rpc_srv = nil
      @rpc_thread = nil
      @stop_pull = false

      @parser = Plugin.new_parser(@format)
      @parser.configure(conf)
    end

    def start
      super
      start_rpc if @enable_rpc

      @subscriber = Fluent::GcloudPubSub::Subscriber.new @project, @key, @topic, @subscription
      log.debug "connected subscription:#{@subscription} in project #{@project}"

      @stop_subscribing = false
      @subscribe_thread = Thread.new(&method(:subscribe))
    end

    def shutdown
      super
      if @rpc_srv
        @rpc_srv.shutdown
        @rpc_srv = nil
      end
      if @rpc_thread
        @rpc_thread.join
        @rpc_thread = nil
      end
      @stop_subscribing = true
      @subscribe_thread.join
    end

    def stop_pull
      @stop_pull = true
      log.info "stop pull from subscription:#{@subscription}"
    end

    def start_pull
      @stop_pull = false
      log.info "start pull from subscription:#{@subscription}"
    end

    private

    def start_rpc
      log.info "listening http rpc server on http://#{@rpc_bind}:#{@rpc_port}/"
      @rpc_srv = WEBrick::HTTPServer.new(
        {
          BindAddress: @rpc_bind,
          Port: @rpc_port,
          Logger: WEBrick::Log.new(STDERR, WEBrick::Log::FATAL),
          AccessLog: []
        }
      )
      @rpc_srv.mount('/api/in_gcloud_pubsub/pull/', RPCServlet, self)
      @rpc_thread = Thread.new {
        @rpc_srv.start
      }
    end

    def subscribe
      until @stop_subscribing
        _subscribe unless @stop_pull

        if @return_immediately || @stop_pull
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
