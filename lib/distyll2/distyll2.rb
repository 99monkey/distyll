module Distyll2
  class << self
    attr_accessor :configuration
  end

  def self.configure
    self.configuration ||= Configuration.new
    yield(configuration)
  end

  class Configuration
    attr_accessor :models, :db

    def initialize
      self.models = []
    end

    def add_model(name, opts={})
      limit = opts[:limit] || 10000
      created_at_since = opts[:created_at_since] || 1.month.ago

      self.models << {
          name: name,
          limit: limit,
          created_at_since: created_at_since,
      }
    end
  end
end