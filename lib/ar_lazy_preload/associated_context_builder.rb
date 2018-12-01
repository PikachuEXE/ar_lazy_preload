# frozen_string_literal: true

require "set"
require "ar_lazy_preload/association_tree_builder"

module ArLazyPreload
  # This class is responsible for building context for associated records. Given a list of records
  # belonging to the same context and association name it will create and attach a new context to
  # the associated records based on the parent association tree.
  class AssociatedContextBuilder
    # Initiates lazy preload context the records loaded lazily
    def self.prepare(*args)
      new(*args).perform
    end

    attr_reader :parent_context, :association_name

    # :parent_context - root context
    # :association_name - lazily preloaded association name
    def initialize(parent_context:, association_name:)
      @parent_context = parent_context
      @association_name = association_name
    end

    # Takes all the associated records for the records, attached to the :parent_context and creates
    # a preloading context for them
    def perform
      enumerator = AssociationArrayLikeEnumerator.new(
        parent_context.records,
        association_name,
      )

      Context.register(
        records: enumerator,
        association_tree: child_association_tree,
      )
    end

    private

    def child_association_tree
      # `association_tree` is unnecessary when auto preload is enabled
      return nil if ArLazyPreload.config.auto_preload?

      AssociationTreeBuilder.new(parent_context.association_tree).subtree_for(association_name)
    end

    class AssociationArrayLikeEnumerator
      def initialize(parent_records, association_name, compact: false)
        @parent_records = parent_records
        @association_name = association_name
        @compact = compact
      end

      def each(*args, &block)
        enumerator.each(*args, &block)
      end

      def map(*args, &block)
        enumerator.map(*args, &block)
      end

      def all?
        enumerator.each do |rec|
          result = yield rec
          return false unless result
        end

        true
      end

      attr_reader :uniq_records
      alias_method :uniq_records?, :uniq_records

      # Cache size to avoid performance issue on repeated calls
      def size
        called_records = uniq_records? ? Set.new : nil

        @size ||= @parent_records.map do |record|
          next 0 if record.nil?

          record_association = record.association(@association_name)
          if record_association.reflection.collection?
            record_association.target.count do |asso_rec|
              if uniq_records?
                next false if called_records.include?(asso_rec)
                called_records.add(asso_rec)
              end

              true
            end
          else
            next 1 unless compact
            if uniq_records?
              next 0 if called_records.include?(record_association.target)
              called_records.add(record_association.target)
            end
            record_association.target.nil? ? 0 : 1
          end
        end.sum
      end

      def empty?
        size.zero?
      end

      # This method is for dealing with `Array.wrap` used in `#grouped_record`
      # used inside`ActiveRecord::Associations::Preloader#preload`
      # to avoid the enumerator to be wrapped inside an array
      def to_ary
        self
      end

      def compact
        AssociationArrayLikeEnumerator.new(
          @parent_records,
          @association_name,
          compact: true,
        )
      end

      def uniq!
        @uniq_records = true
        # Invalidate cache
        @size = nil
        @enumerator = nil
      end

      private

      def enumerator
        @enumerator ||= Enumerator.new(size) do |y|
          called_records = uniq_records? ? Set.new : nil

          @parent_records.each do |record|
            next if record.nil?

            record_association = record.association(@association_name)
            if record_association.reflection.collection?
              record_association.target.each do |asso_rec|
                if uniq_records?
                  next if called_records.include?(asso_rec)
                  called_records.add(asso_rec)
                end

                y.yield(asso_rec)
              end
            else
              next if compact && record_association.target.nil?
              if uniq_records?
                next if called_records.include?(record_association.target)
                called_records.add(record_association.target)
              end

              y.yield(record_association.target)
            end
          end
        end
      end
    end
    private_constant :AssociationArrayLikeEnumerator
  end
end
