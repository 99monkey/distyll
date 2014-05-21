class Distyll

  attr_reader :base_models, :model_forest, :created_since

  def initialize(bms, cs)
    @created_since = cs.to_date
    @base_models = bms.map &:constantize
    @model_forest = Distyll.build_model_forest(base_models)
  end

  def run
    base_models.each do |base_model|
      records = base_model.where("created_at >= ?", created_since).order(:id).load
      run_on_associations(records)
      copy_records(records)
    end
  end

  def self.build_model_forest(model_array)
    tree = {}
    model_array.each { |m| tree[m] = build_model_tree(m) }
    tree
  end

  def self.build_model_tree(model)
    branch = {}
    model.reflect_on_all_associations.each do |association|
      if association.belongs_to? && association.through_reflection.nil?
        branch[association.klass] = build_model_tree(association.klass)
      end
    end
    branch
  end


  private

  #TODO: keep track of which models have already been traversed, add to those.
  #TODO: "oh, crap" on self-referential joins
  def run_on_associations(records)
    return if records.blank?

    model = records.first.class
    puts "Starting Associations for #{model.to_s}"

    model.reflect_on_all_associations.each do |association|
      if association.belongs_to? && association.through_reflection.nil?
        associated_records = get_associated_records(records, association)
        run_on_associations(associated_records)
        copy_records(associated_records)
      end
    end
  end

  def copy_records(records)
    return if records.blank?

    model = records.first.class
    puts "Copying Records for #{model.to_s}"

    model.establish_connection("distyll")
    records.each do |record|
      model.create!(record.attributes)
    end
    model.establish_connection(Rails.env)
  end

  def get_associated_records(records, association)
    puts "Getting Associated #{association.name.to_s.titleize.pluralize}"

    associated_records = []
    records.each do |r|
      associated_records << r.send(association.name) #TODO: only works for belongs_to.  Need to consider += for other association types
    end
    associated_records.compact.uniq
  end

end