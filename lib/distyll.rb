class Distyll

  attr_reader :base_models, :model_profiles, :created_since

  def initialize(bms, cs)
    @created_since = cs.to_date
    @base_models = bms.map &:constantize
    set_model_profiles
  end

  def run
    base_models.each do |model|
      @model_profiles[model].load_ids_by_timestamp(@created_since)
    end

    prior_count = -1
    while prior_count != current_count
      prior_count = current_count
      @model_profiles.each_value &:demote_new_ids

      @model_profiles.each_value do |profile|
        profile.associations.each do |a|
          @model_profiles[a.klass].load_ids(profile.get_new_associated_ids(a))
        end
      end
    end

    @model_profiles.each_value do |profile|
      profile.copy_records
    end
  end


  private

  def set_model_profiles
    @model_profiles = Hash.new
    base_models.each do |bm|
      @model_profiles = potentially_add_profiles(bm, @model_profiles)
    end
  end

  def potentially_add_profiles(model, profiles)
    return profiles if profiles.include? model
    profiles[model] = DistyllModelProfile.new(model)
    profiles[model].associations.each do |a|
      profiles = potentially_add_profiles(a.klass, profiles)
    end
    profiles
  end

  def current_count
    model_profiles.each_value.sum &:get_id_count
  end

end



class DistyllModelProfile
  attr_reader :model, :record_count, :associations, :all_ids, :last_ids, :new_ids

  def initialize(m)
    @model = m
    @record_count = m.count
    @all_ids = Array.new
    @last_ids = Array.new
    @new_ids = Array.new
    set_associations
  end

  def demote_new_ids
    @last_ids = @new_ids
    @new_ids = Array.new
  end

  def load_ids_by_timestamp(timestamp)
    ids = model.where("created_at >= ?", timestamp).select(:id).map &:id
    @new_ids += ids
    @all_ids += ids
  end

  def load_ids(ids)
    @new_ids += ids
    @all_ids += ids
  end

  def get_id_count
    @all_ids = @all_ids.uniq || []
    @all_ids.count
  end

  def get_new_associated_ids(a)
    model.where(id: last_ids).select(a.foreign_key).map { |r| r.send(a.foreign_key) }
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