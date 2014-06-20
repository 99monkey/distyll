class Distyll

  def self.run(base_models, created_since)
    @model_profiles = Hash.new

    base_models.each do |model|
      if @model_profiles[model].nil?
        base_profile = DistyllModelProfile.new(model, true)
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
            # We DO want to make the associated profile continue to traverse has_manies if
            #   (a) The current profile traverses has_manies AND
            #   (b) The association we're about to traverse is a has_many.
            contagious_has_many = profile.include_has_many && !(a.belongs_to? || a.has_and_belongs_to_many?)

            find_or_store_profile(a.klass, contagious_has_many).load_ids(profile.get_new_associated_ids(a))
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

  def self.find_or_store_profile(model, include_has_many)
    @model_profiles[model] ||= DistyllModelProfile.new(model, include_has_many)
  end

end



class DistyllModelProfile
  attr_reader :model, :include_has_many, :record_count, :associations, :all_ids, :prior_ids, :current_ids

  def initialize(m, include_h_m = false)
    @model = m
    @include_has_many = include_h_m
    @record_count = m.count
    @all_ids = Set.new
    @prior_ids = Set.new
    @current_ids = Set.new
    set_associations
  end

  def demote_current_ids
    @prior_ids = @current_ids
    @current_ids = Set.new
  end

  def load_ids_by_timestamp(timestamp)
    ids = model.where("created_at >= ?", timestamp).select(:id).map &:id
    @current_ids.merge(ids)
    @all_ids.merge(ids)
  end

  def load_ids(ids)
    @current_ids.merge(ids)
    @all_ids.merge(ids)
  end

  def get_id_count
    @all_ids.count
  end

  # This is the first method in which chunking for Oracle's 1000 IN limit would be necessary
  def get_new_associated_ids(a)
    if a.belongs_to?
      model.where(id: prior_ids.to_a).select(a.foreign_key).map { |r| r.send(a.foreign_key) }
    else
      # Polymorphism could slow us down here, causing us to pull more records than we want to.
      a.klass.where(a.foreign_key => prior_ids.to_a).select(:id).map { |r| r.send(:id) }
    end
  end

  # This is the second method in which chunking for Oracle's 1000 IN limit would be necessary
  def copy_records
    return nil if all_ids.blank?

    records = model.where(id: all_ids.to_a).load

    model.establish_connection("distyll")
    records.each { |record| model.new(record.attributes).save!(validate: false) }
    model.establish_connection(Rails.env)

    records
  end

  private

  def set_associations
    @associations = Array.new
    model.reflect_on_all_associations.each do |association|
      if association.through_reflection.nil?
        if association.belongs_to? || self.include_has_many
          @associations << association
        end
      end
    end
  end

end