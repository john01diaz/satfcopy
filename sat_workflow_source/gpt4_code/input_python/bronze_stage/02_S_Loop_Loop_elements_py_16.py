
layer_with_loop_elements = (
    layer_with_loop
    .join(both_class_loop_elements_df,
        (layer_with_loop.loop_database_name == both_class_loop_elements_df.loop_element_database_name)&
        (layer_with_loop.CS_Loop_CS_Loop_element_dyn_class == both_class_loop_elements_df.loop_element_dynamic_class)&
        (layer_with_loop.CS_Loop_CS_Loop_element_href == both_class_loop_elements_df.loop_element_object_identifier)
        ,'inner'
    )
)


