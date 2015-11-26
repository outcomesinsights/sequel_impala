require 'securerandom'
require 'shellwords'

module Sequel::CsvToParquet
  # Load a CSV file into an existing parquet table.  By default,
  # assumes the CSV file has headers that match the column names
  # in the parquet table.  If this isn't true, the :headers or
  # :mapping option should be specified.
  #
  # This works by adding the CSV file to HDFS via hdfs -put, then
  # creating an external CSV table in Impala, then inserting into
  # parquet table from the CSV table.
  #
  # Options:
  # :empty_null :: Convert empty CSV cells to \N when adding to HDFS,
  #                so Impala will treat them as NULL instead of the
  #                empty string.
  # :headers :: Specify the headers to use in the CSV file, assuming the
  #             csv file does not contain headers.  If :skip_headers is set
  #             to true, this will ignore the existing headers in the file.
  # :hdfs_tmp_dir :: The temporary HDFS directory to use when uploading.
  # :mapping :: Override the mapping of the CSV columns to the parquet table
  #             columns.  By default, assumes the CSV header names are the
  #             same as the parquet table columns, and uses both.  If specified
  #             this should be a hash with parquet column symbol keys, with the
  #             value being the value to insert into the parquet table.  This
  #             can be used to transform the data from the CSV table when loading
  #             it into the parquet table.
  # :overwrite :: Set to true to overwrite existing data in the parquet table
  #               with the information from the CSV file.  The default is to
  #               append the data to the existing parquet table.
  # :skip_header :: Specifies that the first row contains headers and should
  #                 be skipped when copying the CSV file to HDFS.  If not
  #                 specified, headers are skipped unless the :headers option
  #                 is given.
  # :tmp_table :: The temporary table name to use for the CSV table.
  # :types :: Specify the types to use for the temporary CSV table. By default,
  #           it introspects the parquet table to get the type information, and
  #           uses the type for the matching column name.
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

    pipeline = if skip_header
      "tail -n +2 #{Shellwords.shellescape(local_csv_path)}"
    else
      "cat #{Shellwords.shellescape(local_csv_path)}"
    end

    case opts[:empty_null]
    when nil, false
    when :perl
      pipeline << ' | perl -p -e \'s/(^|,)(?=,|$)/\\1\\\\N/g\'' 
    else
      pipeline << (' | sed -r \'s/(^|,)(,|$)/\\1\\\\N\\2/g\'' * 2 )
    end

    system("#{pipeline} | hdfs dfs -put - #{Shellwords.shellescape(hdfs_tmp_file)}")

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
