class Distyll

  attr_accessor :base_model, :created_since

  def initialize(bm, cs)
    @base_model = bm.constantize #TODO: Allow more than one base model
    @created_since = cs.to_date
  end

  def run
    records = base_model.where("created_at >= ?", created_since).order(:id)
    run_on_associations(base_model, records)
    copy_records(base_model, records)
  end

  private

  #TODO: keep track of which models have already been traversed, add to those.
  def run_on_associations(model, records)
    puts "Starting Associations for #{model.to_s}"
    model.reflect_on_all_associations.each do |a|
      if a.belongs_to? #TODO: and not a through
        associated_records = get_associated_records(records, a)
        run_on_associations(a.klass, associated_records)
        copy_records(a.klass, associated_records)
      end
    end
  end

  def copy_records(model, records)
    puts "Copying Records for #{model.to_s}"
    records = records.to_a
    model.establish_connection("distyll") #TODO: get model off of records rather than passing it in?
    records.each do |record|
      model.create!(record.attributes)
    end
    model.establish_connection(Rails.env)
  end

  def get_associated_records(records, association)
    puts "Getting Associated #{association.name.to_s.titleize.pluralize}"
    associated_records = []
    records.each do |r|
      associated_records << r.send(association.name) #TODO: only works for belongs_to.  Need to consider +=
    end
    associated_records.compact.uniq
  end

end