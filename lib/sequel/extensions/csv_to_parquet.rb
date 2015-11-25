require 'securerandom'
require 'shellwords'

module Sequel::CsvToParquet
  def load_csv(local_csv_path, into_table, opts={})
    tmp_num = SecureRandom.hex(8)
    hdfs_tmp_dir = opts[:hdfs_tmp_dir] || "/tmp/cvs-#{tmp_num}"
    hdfs_tmp_file = "#{hdfs_tmp_dir}/#{File.basename(local_csv_path)}"
    tmp_table = opts[:tmp_table] || "csv_#{tmp_num}"

    skip_header = opts.fetch(:skip_header, !opts.has_key?(:headers))
    mapping = opts[:mapping]
    overwrite = opts[:overwrite]

    if columns = opts[:headers]
      columns = columns.split(',') if columns.is_a?(String)
    else
      columns = File.open(local_csv_path).readline.chomp.split(',').map(&:downcase).map(&:to_sym)
    end

    into_table_columns = describe(into_table) rescue nil

    if types = opts[:types]
      types = types.split(',') if types.is_a?(String)
    elsif (into_table_columns)
      sch = Hash[into_table_columns.map { |h| [h[:name].downcase.to_sym, h[:type]]}]
      types = columns.map { |col| sch[col] || "string" }
    else
      types = ["string"] * columns.length
    end

    unless types.length == columns.length
      raise ArgumentError, "number of types doesn't match number of columns"
    end

    system("hdfs", "dfs", "-mkdir", hdfs_tmp_dir)

    if skip_header
      system("tail -n +2 #{Shellwords.shellescape(local_csv_path)} | hdfs dfs -put - #{Shellwords.shellescape(hdfs_tmp_file)}")
    else
      system("hdfs", "dfs", "-put", local_csv_path, hdfs_tmp_file)
    end

    create_table(tmp_table, :external=>true, :field_term=>',', :location=>hdfs_tmp_dir) do
      columns.zip(types) do |c, t|
        column c, t
      end
    end

    ds = from(into_table)
    ds = ds.insert_overwrite if overwrite

    if mapping
      table_columns, csv_columns = mapping.to_a.transpose
    else
      table_columns = csv_columns = into_table_columns.map { |h| h[:name].to_sym }
    end
    ds.insert(table_columns, from(tmp_table).select(*csv_columns))

  ensure
    system("hdfs", "dfs", "-rm", hdfs_tmp_file)
    system("hdfs", "dfs", "-rmdir", hdfs_tmp_dir)
    drop_table?(tmp_table)
  end
end

Sequel::Database.register_extension(:csv_to_parquet, Sequel::CsvToParquet)
