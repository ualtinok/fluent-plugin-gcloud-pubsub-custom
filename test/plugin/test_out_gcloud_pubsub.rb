require_relative "../test_helper"

class GcloudPubSubOutputTest < Test::Unit::TestCase
  CONFIG = %[
    project project-test
    topic topic-test
    key key-test
  ]

  ReRaisedError = Class.new(RuntimeError)

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::GcloudPubSubOutput).configure(conf)
  end

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'configure' do
    test 'default values are configured' do
      d = create_driver(%[
        project project-test
        topic topic-test
        key key-test
      ])

      assert_equal('project-test', d.instance.project)
      assert_equal('topic-test', d.instance.topic)
      assert_equal('key-test', d.instance.key)
      assert_equal(false, d.instance.autocreate_topic)
      assert_equal(1000, d.instance.max_messages)
      assert_equal(4000000, d.instance.max_total_size)
    end

    test '"topic" must be specified' do
      assert_raises Fluent::ConfigError do
        create_driver(%[
          project project-test
          key key-test
        ])
      end
    end

    test '"autocreate_topic" can be specified' do
      d = create_driver(%[
        project project-test
        topic topic-test
        key key-test
        autocreate_topic true
      ])

      assert_equal(true, d.instance.autocreate_topic)
    end
  end

  sub_test_case 'topic' do
    setup do
      @publisher = mock!
      @pubsub_mock = mock!
      stub(Google::Cloud::Pubsub).new { @pubsub_mock }
    end

    test '"autocreate_topic" is enabled' do
      d = create_driver(%[
        project project-test
        topic topic-test
        key key-test
        autocreate_topic true
      ])

      @pubsub_mock.topic("topic-test", autocreate: true).once { @publisher }
      d.run
    end

    test '40x error occurred on connecting to Pub/Sub' do
      d = create_driver

      @pubsub_mock.topic('topic-test', autocreate: false).once do
        raise Google::Cloud::NotFoundError.new('TEST')
      end

      assert_raise Google::Cloud::NotFoundError do
        d.run {}
      end
    end

    test '50x error occurred on connecting to Pub/Sub' do
      d = create_driver

      @pubsub_mock.topic('topic-test', autocreate: false).times(5) do
        raise Google::Cloud::UnavailableError.new('TEST')
      end

      assert_raise Google::Cloud::UnavailableError do
        d.run {}
      end
    end

    test 'topic is nil' do
      d = create_driver

      @pubsub_mock.topic('topic-test', autocreate: false).once { nil }

      assert_raise Fluent::GcloudPubSub::Error do
        d.run {}
      end
    end
  end

  sub_test_case 'publish' do
    setup do
      @publisher = mock!
      @pubsub_mock = mock!.topic(anything, anything) { @publisher }
      stub(Google::Cloud::Pubsub).new { @pubsub_mock }
    end

    setup do
      @time = Time.parse('2016-07-09 11:12:13 UTC').to_i
    end

    test 'messages are divided into "max_messages"' do
      d = create_driver
      @publisher.publish.times(2)
      # max_messages is default 1000
      1001.times do |i|
        d.emit({"a" => i}, @time)
      end
      d.run
    end

    test 'messages are divided into "max_total_size"' do
      d = create_driver(%[
        project project-test
        topic topic-test
        key key-test
        max_messages 100000
        max_total_size 1000
      ])

      @publisher.publish.times(2)
      # 400 * 4 / max_total_size = twice
      4.times do
        d.emit({"a" => "a" * 400}, @time)
      end
      d.run
    end

    test 'accept "ASCII-8BIT" encoded multibyte strings' do
      # on fluentd v0.14, all strings treated as "ASCII-8BIT" except specified encoding.
      d = create_driver
      @publisher.publish.once
      d.emit({"a" => "あああ".force_encoding("ASCII-8BIT")}, @time)
      d.run
    end

    test 'reraise unexpected errors' do
      d = create_driver
      @publisher.publish.once { raise ReRaisedError }
      assert_raises ReRaisedError do
        d.emit([{'a' => 1, 'b' => 2}])
        d.run
      end
    end

    test 'reraise RetryableError' do
      d = create_driver
      @publisher.publish.once { raise Google::Cloud::UnavailableError.new('TEST') }
      assert_raises Fluent::GcloudPubSub::RetryableError do
        d.emit([{'a' => 1, 'b' => 2}])
        d.run
      end
    end
  end
end
