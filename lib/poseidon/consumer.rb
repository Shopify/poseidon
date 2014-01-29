require 'thread'

module Poseidon
  # A Kafka Consumer that is as easy to use as possible.
  # Automatically connects to all the partitions of a topic,
  # and implements Enumerable for easy consuming.
  #
  # ## Example
  #
  # consumer = Poseidon::Consumer.new("test_client", %w(localhost:9092), "test_topic", :earliest_offset)
  # first_five = consumer.take(5)
  # # Consume indefinitely
  # consumer.each { |message| puts message.value }
  #
  # @api public
  class Consumer
    include Enumerable

    attr_reader :client_id, :seed_brokers, :topic, :offset, :options

    # Create a Consumer that reads from all the partitions of a topic at once
    #
    # @param [String] client_id
    #   Used to identify this client. Should be unique.
    #
    # @param [Enumerable<String>] seed_brokers
    #   The initial brokers that are used for fetching the metadata.
    #
    # @param [Integer,Symbol] offset
    #   Offset to start reading from.
    #   There are a couple special offsets which can be passed as symbols:
    #     :earliest_offset Start reading from the first offset the server has.
    #     :latest_offset   Start reading from the latest offset the server has.
    #
    # @param [Hash] options
    #   Theses options are passed to the individual PartitionConsumers
    #
    # @option options [:max_bytes] Maximum number of bytes to fetch
    #   Default: 1048576 (1MB)
    # @option options [:max_wait_ms]
    #   How long to block until the server sends us data.
    #   Default: 100 (100ms)
    # @option options [:min_bytes] Smallest amount of data the server should send us.
    #   Default: 0 (Send us data as soon as it is ready)
    #
    def initialize(client_id, seed_brokers, topic, offset, options = {})
      @client_id = client_id
      @seed_brokers = seed_brokers
      @topic = topic
      @offset = offset
      @options = options

      @queue = Queue.new
    end

    # Start fetching messages from all partitions at once. Expects a block
    #
    # You can restart consuming after stopping it before (by breaking inside
    # the block for example) and the consumer will resume where it left off.
    #
    # @api public
    def each
      @partition_consumers ||= create_partition_consumers

      begin
        threads = @partition_consumers.map do |partition_consumer|
          Thread.new(Thread.current) do |parent|
            begin
              loop do
                fetched_messages = partition_consumer.fetch

                fetched_messages.each do |fetched_message|
                  @queue << fetched_message
                end
              end
            rescue => e
              parent.raise e
            end
          end
        end

        loop do
          yield @queue.pop
        end
      ensure
        threads.each(&:kill)
        threads.each(&:join)
      end
    end

    alias_method :consume, :each

    private
    def create_partition_consumers
      cluster_metadata = fetch_cluster_metadata

      topic_metadata = cluster_metadata.topic_metadata[topic]
      (0...topic_metadata.partition_count).map do |partition|
        lead_broker = cluster_metadata.lead_broker_for_partition(topic, partition)

        PartitionConsumer.new(client_id, lead_broker.host, lead_broker.port, topic, partition, offset, options)
      end
    end

    def fetch_cluster_metadata
      broker_pool = BrokerPool.new(client_id, seed_brokers)

      cluster_metadata = ClusterMetadata.new
      cluster_metadata.update(broker_pool.fetch_metadata([topic]))
      cluster_metadata
    end
  end
end
