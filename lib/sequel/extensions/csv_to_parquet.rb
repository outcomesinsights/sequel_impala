require 'securerandom'
require 'csv'
require 'open3'

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
  #                empty string.  Defaults to using 2 sed processes to
  #                convert empty cells.  Can be set to :perl to use a
  #                single perl process.  Can be set to :ruby to do the
  #                processing inside the ruby process, which will also
  #                convert quoted CSV cells (which Hive/Impala do not
  #                support) to escaped CSV cells (which Hive/Impala do
  #                support).
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

    raw_data = File.open(local_csv_path, 'rb')

    if columns = opts[:headers]
      columns = columns.split(',') if columns.is_a?(String)
      raw_data.readline if skip_header
    else
      columns = raw_data.readline.chomp.split(',').map(&:downcase).map(&:to_sym)
    end
    raw_data.seek(raw_data.pos, IO::SEEK_SET)

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
    hdfs_put = ['hdfs', 'dfs', '-put', '-', hdfs_tmp_file]

    case opts[:empty_null]
    when nil, false
      system(*hdfs_put, :in=>raw_data)
    when :ruby
      error_in_thread = nil
      csv_data, input = IO.pipe
      csv_thread = Thread.new do
        begin
          comma = ','.freeze
          comma_rep = '\\,'.freeze
          nl = "\n".freeze
          null = '\\N'.freeze
          empty = ''.freeze

          write_col = lambda do |col, after|
            if !col || col == empty
              col = null
            else
              col.gsub!(nl, empty)
              col.gsub!(comma, comma_rep)
            end
            input.write(col)
            input.write(after)
          end

          raw_data.seek(0, IO::SEEK_SET)
          CSV.open(raw_data) do |csv|
            csv.shift if skip_header
            csv.each do |row|
              last = row.pop
              row.each do |col|
                write_col.call(col, comma)
              end
              write_col.call(last, nl)
            end
          end
        ensure
          input.close
          csv_data.close
        end
      end
      system(*hdfs_put, :in=>csv_data)
      csv_thread.join
    when :perl
      Open3.pipeline(
        ['perl', '-p', '-e', 's/(^|,)(?=,|$)/\\1\\\\N/g', {:in=>raw_data}],
        hdfs_put
      )
    else
      Open3.pipeline(
        ['sed', '-r', 's/(^|,)(,|$)/\\1\\\\N\\2/g', {:in=>raw_data}],
        ['sed', '-r', 's/(^|,)(,|$)/\\1\\\\N\\2/g'],
        hdfs_put
      )
    end

    create_table(tmp_table, :external=>true, :field_term=>',', :field_escape=>'\\', :location=>hdfs_tmp_dir) do
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
    raw_data.close if raw_data && !raw_data.closed?

    system("hdfs", "dfs", "-rm", hdfs_tmp_file)
    system("hdfs", "dfs", "-rmdir", hdfs_tmp_dir)
    drop_table?(tmp_table)
  end
end

Sequel::Database.register_extension(:csv_to_parquet, Sequel::CsvToParquet)
