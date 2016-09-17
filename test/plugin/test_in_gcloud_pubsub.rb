require_relative "../test_helper"

class GcloudPubSubInputTest < Test::Unit::TestCase
  CONFIG = %[
      tag test
      project project-test
      topic topic-test
      subscription subscription-test
      key key-test
      format json
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::GcloudPubSubInput).configure(conf)
  end

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'configure' do
    test 'all params are configured' do
      d = create_driver(%[
        tag test
        project project-test
        topic topic-test
        subscription subscription-test
        key key-test
        max_messages 1000
        return_immediately true
        pull_interval 2
        format ltsv
      ])

      assert_equal('test', d.instance.tag)
      assert_equal('project-test', d.instance.project)
      assert_equal('topic-test', d.instance.topic)
      assert_equal('subscription-test', d.instance.subscription)
      assert_equal('key-test', d.instance.key)
      assert_equal(2, d.instance.pull_interval)
      assert_equal(1000, d.instance.max_messages)
      assert_equal(true, d.instance.return_immediately)
      assert_equal('ltsv', d.instance.format)
    end

    test 'default values are configured' do
      d = create_driver
      assert_equal(5, d.instance.pull_interval)
      assert_equal(100, d.instance.max_messages)
      assert_equal(true, d.instance.return_immediately)
      assert_equal('json', d.instance.format)
    end
  end

  sub_test_case 'emit' do
    class DummyMsgData
      def data
        return '{"foo": "bar"}'
      end
    end
    class DummyMessage
      def message
        DummyMsgData.new
      end
    end

    setup do
      @subscriber = mock!
      @topic_mock = mock!.subscription('subscription-test') { @subscriber }
      @pubsub_mock = mock!.topic('topic-test') { @topic_mock }
      @gcloud_mock = mock!.pubsub { @pubsub_mock }
      stub(Google::Cloud).new { @gcloud_mock }
    end

    test 'empty' do
      @subscriber.pull(immediate: true, max: 100).once { [] }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }

      assert_equal(true, d.emits.empty?)
    end

    test 'simple' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).once { messages }
      @subscriber.acknowledge(messages).once

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }
      emits = d.emits

      assert_equal(1, emits.length)
      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'invalid messages' do
      class DummyInvalidMsgData
        def data
          return 'foo:bar'
        end
      end
      class DummyInvalidMessage
        def message
          DummyInvalidMsgData.new
        end
      end

      messages = Array.new(1, DummyInvalidMessage.new)
      @subscriber.pull(immediate: true, max: 100).once { messages }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }
      assert_equal(true, d.emits.empty?)
    end

    test 'retry if raised error' do
      class UnknownError < StandardError
      end
      @subscriber.pull(immediate: true, max: 100).twice { raise UnknownError.new('test') }
      @subscriber.acknowledge.times(0)

      d = create_driver(CONFIG + 'pull_interval 1')
      d.run {
        sleep 1 # + 0.5s
      }

      assert_equal(true, d.emits.empty?)
    end
  end
end
