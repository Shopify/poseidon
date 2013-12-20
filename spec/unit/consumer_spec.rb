require 'spec_helper'

describe Consumer do
  describe "creation" do
    it "creates a consumer" do
      consumer = Poseidon::Consumer.new("test_client", %w(localhost:9092), "test_topic", :earliest_offset)
    end
  end

  describe "#create_partition_consumers" do
    it "fetches metadata for the topic to create the consumers" do
      topic = "test_topic"
      offset = :earliest_offset
      min_bytes = 20

      partitions = [
        PartitionMetadata.new(nil, 0, 1, [0, 1], [0, 1]),
        PartitionMetadata.new(nil, 1, 2, [1, 0], [1, 0])
      ]
      topics = [TopicMetadata.new(TopicMetadataStruct.new(nil, topic, partitions))]
      brokers = [Broker.new(1, "localhost", 9092), Broker.new(2, "localhost", 9094)]
      metadata_response = MetadataResponse.new(nil, brokers, topics)

      cluster_metadata = ClusterMetadata.new
      cluster_metadata.update(metadata_response)


      consumer = Poseidon::Consumer.new("test_client", %w(localhost:9092), topic, offset, :min_bytes => min_bytes)
      consumer.stub(:fetch_cluster_metadata).and_return(cluster_metadata)

      partition_consumers = consumer.send(:create_partition_consumers)

      partition_consumers.each_with_index do |partition_consumer, index|
        expect(partition_consumer.instance_variable_get(:@partition)).to eq(index)
        expect(partition_consumer.instance_variable_get(:@topic)).to eq(topic)
        expect(partition_consumer.instance_variable_get(:@offset)).to eq(offset)
        expect(partition_consumer.instance_variable_get(:@host)).to eq("localhost")
        expect(partition_consumer.instance_variable_get(:@min_bytes)).to eq(min_bytes)
      end
      expect(partition_consumers[0].instance_variable_get(:@port)).to eq(9092)
      expect(partition_consumers[1].instance_variable_get(:@port)).to eq(9094)
    end
  end
end
