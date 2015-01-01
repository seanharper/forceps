module Forceps
  module ActsAsCopyableModel
    extend ActiveSupport::Concern

    def copy_to_local
      without_record_timestamps do
        DeepCopier.new(forceps_options).copy(self)
      end
    end

    private

    def without_record_timestamps
      self.class.base_class.record_timestamps = false
      yield
    ensure
      self.class.base_class.record_timestamps = true
    end

    def forceps_options
      Forceps.client.options
    end

    class DeepCopier
      attr_accessor :options, :level

      def initialize(options)
        @copied_remote_objects = {}
        @options = options
        @level = 0
      end

      def copy(remote_object, association_attributes={}, path=[])
        @initial_object ||= remote_object

        detect_loops(remote_object, path)

        if was_copied?(remote_object)
          cached_local_copy(remote_object)
        else
          belongs_to_association_attributes = copy_associated_objects_in_belongs_to(remote_object, path)
          perform_copy(remote_object, association_attributes.merge(belongs_to_association_attributes), path)
        end
      end

      private

      def cached_local_copy(remote_object)
        cached_object = local_copy_for(remote_object)
        debug "#{as_trace(remote_object)} from cache..." if cached_object
        cached_object
      end

      def perform_copy(remote_object, association_attributes, path)
        found_copy = find_local_copy_with_simple_attributes(remote_object, association_attributes) if should_reuse_local_copy?(remote_object)
        local_copy = found_copy || create_local_copy_with_simple_attributes(remote_object, association_attributes)
        store_local_copy_for(remote_object, local_copy)
        copy_associated_objects(local_copy, remote_object, path) unless found_copy
        to_summary(local_copy)
      end

      def was_copied?(remote_object)
        local_copy_for(remote_object)
      end

      def store_local_copy_for(remote_object, local_copy)
        @copied_remote_objects[to_summary(remote_object)] = to_summary(local_copy)
      end

      def local_copy_for(remote_object)
        @copied_remote_objects[to_summary(remote_object)]
      end

      def to_summary(target_object)
        { class: target_object.class, id: target_object.id }
      end

      def should_reuse_local_copy?(remote_object)
        finders_for_reusing_classes[remote_object.class.base_class].present?
      end

      def finders_for_reusing_classes
        options[:reuse] || {}
      end

      def find_local_copy_with_simple_attributes(remote_object, association_attributes)
        found_local_object = finder_for_remote_object(remote_object).call(remote_object)
        copy_simple_attributes(found_local_object, remote_object, association_attributes) if found_local_object
        found_local_object
      end

      def finder_for_remote_object(remote_object)
        finder = finders_for_reusing_classes[remote_object.class.base_class]
        finder = build_attribute_finder(remote_object, finder) if finder.is_a? Symbol
        finder
      end

      def build_attribute_finder(remote_object, attribute_name)
        value = remote_object.send(attribute_name)
        lambda do |object|
          object.class.base_class.where(attribute_name => value).first
        end
      end

      def create_local_copy_with_simple_attributes(remote_object, association_attributes)
        debug "#{as_trace(remote_object)} copying..."

        base_class = base_local_class_for(remote_object)

        disable_all_callbacks_for(base_class)

        cloned_object = base_class.new
        copy_attributes(cloned_object, simple_attributes_to_copy(remote_object).merge(association_attributes))
        cloned_object.save!(validate: false)
        invoke_callbacks(:after_each, cloned_object, remote_object)
        cloned_object
      end

      def base_local_class_for(remote_object)
        base_class = remote_object.class.base_class
        if has_sti_column?(remote_object)
          local_type = to_local_class_name(remote_object.type)
          base_class = local_type.constantize rescue base_class
        end
        base_class
      end

      def to_local_class_name(remote_class_name)
        remote_class_name.gsub('Forceps::Remote::', '')
      end

      def has_sti_column?(object)
        object.respond_to?(:type) && object.type.present? && object.type.is_a?(String)
      end

      def invoke_callbacks(callback_name, copied_object, remote_object)
        callback = callbacks_for(callback_name)[copied_object.class]
        return unless callback
        callback.call(copied_object, remote_object)
      end

      def callbacks_for(callback_name)
        options[callback_name] || {}
      end

      # Using setters explicitly to avoid having to mess with disabling mass protection in Rails 3
      def copy_attributes(target_object, attributes_map)
        make_type_attribute_point_to_local_class_if_needed(attributes_map)

        attributes_map.each do |attribute_name, attribute_value|
          target_object.send("#{attribute_name}=", attribute_value) rescue debug("Failed to set '#{attribute_name}='. Different schemas in the remote and local databases? - #{$!}")
        end
      end

      def make_type_attribute_point_to_local_class_if_needed(attributes_map)
        if attributes_map['type'].is_a?(String)
          attributes_map['type'] = to_local_class_name(attributes_map['type'])
        end
      end

      def disable_all_callbacks_for(base_class)
        [:create, :save, :update, :validate, :touch].each { |callback| base_class.reset_callbacks callback }
      end

      def simple_attributes_to_copy(remote_object)
        remote_object.attributes.reject do |attribute_name|
          attributes_to_exclude(remote_object).include? attribute_name.to_sym
        end
      end

      def attributes_to_exclude(remote_object)
        @attributes_to_exclude_map ||= {}
        @attributes_to_exclude_map[remote_object.class.base_class] ||= calculate_attributes_to_exclude(remote_object)
      end

      def calculate_attributes_to_exclude(remote_object)
        local_excludes = ((options[:exclude] && options[:exclude][remote_object.class.base_class]) || []).collect(&:to_sym)
        local_excludes + global_attributes_to_exclude
      end

      def global_attributes_to_exclude
        options[:global_exclude] || [:id]
      end

      def copy_simple_attributes(target_local_object, source_remote_object, association_attributes)
        debug "#{as_trace(source_remote_object)} reusing..."
        # update_columns skips callbacks too but not available in Rails 3
        copy_attributes(target_local_object, simple_attributes_to_copy(source_remote_object).merge(association_attributes))
        target_local_object.save!(validate: false)
      end

      def logger
        Forceps.logger
      end

      def increase_level
        @level += 1
      end

      def decrease_level
        @level -= 1
      end

      def as_trace(remote_object)
        "<#{remote_object.class.base_class.name} - #{remote_object.id}>"
      end

      def debug(message)
        left_margin = "  "*level
        logger.debug "#{left_margin}#{message}"
      end

      def copy_associated_objects(local_object, remote_object, path)
        with_nested_logging do
          [:has_many, :has_one, :has_and_belongs_to_many].each do |association_kind|
            copy_objects_associated_by_association_kind(local_object, remote_object, association_kind, path)
            local_object.save!(validate: false)
          end
        end
      end

      def with_nested_logging
        increase_level
        result = yield
        decrease_level
        result
      end

      def copy_objects_associated_by_association_kind(local_object, remote_object, association_kind, path)
        associations_to_copy(remote_object, association_kind).collect(&:name).each do |association_name|
          send "copy_associated_objects_in_#{association_kind}", local_object, remote_object, association_name, path
        end
      end

      def associations_to_copy(remote_object, association_kind)
        excluded_attributes = attributes_to_exclude(remote_object)
        remote_object.class.reflect_on_all_associations(association_kind).reject do |association|
          association.options[:through] || excluded_attributes.include?(:all_associations) || excluded_attributes.include?(association.name)
        end
      end

      def copy_associated_objects_in_has_many(local_object, remote_object, association_name, path)
        remote_object.send(association_name).find_each do |remote_associated_object|
          copy(remote_associated_object, association_attribute_for(local_object, association_name), path + [[remote_object, association_name]])
        end
      end

      def copy_associated_objects_in_has_one(local_object, remote_object, association_name, path)
        remote_associated_object = remote_object.send(association_name)
        copy(remote_associated_object, association_attribute_for(local_object, association_name), path + [[remote_object, association_name]]) if remote_associated_object
      end

      def copy_associated_objects_in_belongs_to(remote_object, path)
        with_nested_logging do
          associations_to_copy(remote_object, :belongs_to).collect(&:name).reduce({}) do |association_attributes, association_name|
            remote_associated_object = remote_object.send(association_name)
            copied = remote_associated_object ? copy(remote_associated_object, {}, path + [[remote_object, association_name]]) : nil
            association_attributes[remote_object.class.reflect_on_association(association_name).foreign_key] = copied && copied[:id]
            association_attributes
          end
        end
      end

      def copy_associated_objects_in_has_and_belongs_to_many(local_object, remote_object, association_name, path)
        copied_objects = []
        remote_object.send(association_name).find_each do |remote_associated_object|
          cloned_local_associated_object = copy(remote_associated_object, {}, path + [[remote_object, association_name]])
          unless local_object.send(association_name).where(id: cloned_local_associated_object.id).exists?
            copied_objects << cloned_local_associated_object
          end
        end
        local_object.send"#{association_name}=", copied_objects
      end

      def detect_loops(remote_object, path)
        if @initial_object.class == remote_object.class && @initial_object.id != remote_object.id
          "Loop detected - #{(path.map {|(path_object, association_name)| association_debug(path_object, association_name)}).join(', ')} - #{as_trace(remote_object)}".tap do |msg|
            if @options[:raise_on_loops]
              raise msg
            else
              debug msg
            end
          end
        end
      end

      def association_debug(remote_object, association_name)
        "#{as_trace(remote_object)}->#{association_name}"
      end

      def association_attribute_for(object, association_name)
        { object.class.reflect_on_association(association_name).foreign_key => object.id }
      end
    end
  end
end
