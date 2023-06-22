
loop_df_with_document_number =(
    loop_df
    .join(DM_Circuit_diagram_df,
          (loop_df.loop_database_name == DM_Circuit_diagram_df.database_name)&
          (loop_df.loop_dynamic_class == DM_Circuit_diagram_df.CS_Loop_DM_Document_dyn_class)&
          (loop_df.loop_object_identifier == DM_Circuit_diagram_df.CS_Loop_DM_Document_href),
          'left'
         )
    .drop(DM_Circuit_diagram_df.database_name)
    .drop(DM_Circuit_diagram_df.CS_Loop_DM_Document_href)
    .drop(DM_Circuit_diagram_df.CS_Loop_DM_Document_dyn_class)
)

# loop_df_with_document_number.write.format("delta").mode("overwrite").save("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/Loop")

