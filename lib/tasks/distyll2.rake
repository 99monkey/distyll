namespace :distyll2 do
  desc 'run'
  task run: :environment do
    # drop and create db
    Rake::Task['db:drop'].invoke(Distyll2.configure.db)
    Rake::Task['db:schema:load'].invoke(Distyll2.configure.db)

    DistyllModelProfile.db_source_config = Distyll2.configure.db
    Distyll2.configure.models.each do |model|
      Distyll.run model[:name], model[:created_at_since]
    end
  end
end