module Impala
  class ProgressReporter
    def initialize(cursor, options = {})
      @cursor = cursor
      @progress_every = options.fetch(:progress_every, 60).to_f
      @show_progress = options.fetch(:show_progress, false)
      @start_time = @last_progress = Time.now
    end

    def show?
      @show_progress
    end

    def report
      return unless enough_time_elapsed?
      @last_progress = Time.now
      message = sprintf("Progress %.02f%% after %.02f seconds",
                        cursor.progress * 100,
                        total_time_elapsed)
      progress_stream.puts message
    end

  private

    attr :cursor, :show_progress, :last_progress, :progress_every, :start_time

    def progress_stream
      return show_progress if show_progress.is_a?(IO)
      STDERR
    end

    def enough_time_elapsed?
      (Time.now - last_progress) > progress_every
    end

    def total_time_elapsed
      (Time.now - start_time)
    end
  end
end
