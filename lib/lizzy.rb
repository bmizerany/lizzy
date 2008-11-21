require "socket"
require "md5"

require "rubygems"
require "json"
require "mq"

module Lizzy
  extend self

  def default_creds
    @creds ||= {
      :vhost => '/example_vhost',
      :host  => 'localhost',
      :user  => 'guest',
      :pass  => ''
    }
  end

  def set(key, value)
    default_creds[key.to_sym] = value
  end

  def wait_for_reactor(creds, &blk)
    sleep(1) until EM.reactor_running?
    start(creds, &blk)
  end

  def running?
    !!@running
  end

  def start(options = {}, &blk)
    options.merge!(default_creds.merge(options))
    unless EM.reactor_running?
      blk2 = lambda do
        AMQP.start(options) unless running?
        blk.call
        start_console if options[:console]
      end
      EM.run(&blk2)
    else
      AMQP.start(creds) unless running?
      blk.call
      start_console if options[:console]
    end
  end

  def stop_safe
    EM.add_timer(1) { EM.stop_event_loop }
  end

  def start_console
    puts "~ Starting console..."
    require 'readline'
    Thread.new do
      while l = Readline.readline('>> ')
        unless l.nil? or l.strip.empty?
          Readline::HISTORY.push(l)
          begin
            p eval(l, ::TOPLEVEL_BINDING)
          rescue => e
            puts "#{e.class.name}: #{e.message}\n  #{e.backtrace.join("\n  ")}"
          end
        end
      end
    end
  end

  def gensym
    values = [
              rand(0x0010000),
              rand(0x0010000),
              rand(0x0010000),
              rand(0x0010000),
              rand(0x0010000),
              rand(0x1000000),
              rand(0x1000000),
             ]
    "%04x%04x%04x%04x%04x%06x%06x" % values
  end

  class Basic

    def initialize(log = true)
      @log = log
    end

    def silently
      olog, @log = @log, false
      yield
      @log = olog
    end

    def headers
      @headers ||= { :event_hash => self.event_hash }
    end

    def log(level, message, other={})
      return unless @log
      silently do
        publish("logging",
                {
                 :level         => level,
                 :message       => message,
                 :created_at    => Time.now,
                 :component     => :droids,
                 :instance_name => Socket.gethostname,
                 :event_hash    => headers[:event_hash]
                }.merge(other),
                {
                  :queue   => "logger",
                  :durable => true
                })
      end
    end

    def event_hash
      @event_hash ||= begin
        s = Time.now.to_s + self.object_id.to_s + rand(100).to_s
        Digest::MD5.hexdigest(s)
      end
    end

    def publish(message, payload, options={})
      raise "Payload must be a Hash!!!!" unless payload.is_a?(Hash)
      headers[:event_hash] = payload.delete("event_hash") if payload['event_hash']

      data = headers.merge(payload).to_json
      MQ.topic(message, options).publish(data)
      addendum = ""
      addendum = payload.to_json if data.size < 4096
      log(:notice, "#{message} published", :addendum => addendum)
    end

  end

  class Listener < Basic

    def initialize(message, options={})
      @message = message
      @options = options
      super((log = options.delete(:log)).nil? ? true : false)
    end

    def error(e)
      puts msg = "#{e.class}: #{e.message}\n  #{e.backtrace.join("\n  ")}\n"
      log :error, e.message, :addendum => msg
    end

    def defer(&blk)
      EM.defer(lambda do
        begin
          blk.call
        rescue => e
          error(e)
        end
      end)
    end

    def listen(&blk)
      queue = @options.delete(:queue) || Lizzy.gensym
      MQ.queue(queue).bind(MQ.topic(@message, @options)).subscribe do |info, data|
        begin
          req = JSON.parse(data)
          headers[:event_hash] = req["event_hash"] || event_hash
          log :notice, "#{info.exchange} received"
          blk.call(self, info, req)
        rescue => e
          error(e)
        end
      end
    end

  end

  def publish(message, payload, options={})
    Basic.new.publish(message, payload, options)
  end

  def listen4(message, options={}, &blk)
    Listener.new(message, options).listen(&blk)
  end

end
