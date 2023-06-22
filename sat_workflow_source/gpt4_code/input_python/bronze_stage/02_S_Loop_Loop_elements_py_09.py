
layer_with_loop= (
    layer_df
    .join(loop_df,
        (layer_df.layer_database_name == loop_df.loop_database_name)&
        (layer_df.layer_dynamic_class == loop_df.Layer_CS_Loop_dyn_class)&
        (layer_df.layer_object_identifier == loop_df.Layer_CS_Loop_href),
        'inner'
    )
)

