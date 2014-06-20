class Distyll

  def self.run(base_models, created_since)
    @model_profiles = Hash.new

    base_models.each do |model|
      if @model_profiles[model].nil?
        base_profile = DistyllModelProfile.new(model)
        base_profile.load_ids_by_timestamp(created_since)
        @model_profiles[model] = base_profile
      end
    end

    prior_count = -1
    while prior_count != current_count
      prior_count = current_count
      @model_profiles.each_value &:demote_current_ids

      # .dup is necessary here because of Ruby 1.9's "RuntimeError: can't add a new
      #   key into hash during iteration" encountered in self.find_or_store_profile.
      @model_profiles.dup.each_value do |profile|
        unless profile.prior_ids.blank?
          profile.associations.each do |a|
            find_or_store_profile(a.klass).load_ids(profile.get_new_associated_ids(a))
          end
        end
      end
    end

    @model_profiles.each_value do |profile|
      profile.copy_records
    end
  end


  private

  def self.current_count
    @model_profiles.each_value.sum &:get_id_count
  end

  def self.find_or_store_profile(model)
    @model_profiles[model] ||= DistyllModelProfile.new(model)
  end

end



class DistyllModelProfile
  attr_reader :model, :record_count, :associations, :all_ids, :prior_ids, :current_ids

  def initialize(m)
    @model = m
    @record_count = m.count
    @all_ids = Array.new
    @prior_ids = Array.new
    @current_ids = Array.new
    set_associations
  end

  def demote_current_ids
    @prior_ids = @current_ids
    @current_ids = Array.new
  end

  def load_ids_by_timestamp(timestamp)
    ids = model.where("created_at >= ?", timestamp).select(:id).map &:id
    @current_ids += ids
    @all_ids += ids
  end

  def load_ids(ids)
    @current_ids += ids
    @all_ids += ids
  end

  def get_id_count
    @all_ids = @all_ids.uniq || []
    @all_ids.count
  end

  def get_new_associated_ids(a)
    model.where(id: prior_ids).select(a.foreign_key).map { |r| r.send(a.foreign_key) }
  end

  def copy_records
    return nil if all_ids.blank?

    records = model.where(id: all_ids).load

    model.establish_connection("distyll")
    records.each { |record| model.new(record.attributes).save!(validate: false) }
    model.establish_connection(Rails.env)

    records
  end

  private

  def set_associations
    @associations = Array.new
    model.reflect_on_all_associations.each do |association|
      if association.belongs_to? && association.through_reflection.nil?
        @associations << association
      end
    end
  end

end