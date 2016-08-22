require_relative "../test_helper"

class GcloudPubSubInputTest < Test::Unit::TestCase
  CONFIG = %[
      tag test
      project project-test
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
      assert_equal(nil, d.instance.topic)
      assert_equal(5, d.instance.pull_interval)
      assert_equal(100, d.instance.max_messages)
      assert_equal(true, d.instance.return_immediately)
      assert_equal('json', d.instance.format)
    end
  end
end
