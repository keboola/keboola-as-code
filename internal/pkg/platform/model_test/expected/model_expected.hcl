table "branches" {
  schema = schema.main
  column "id" {
    null = false
    type = text
  }
  column "branch_id" {
    null = false
    type = integer
  }
  column "name" {
    null = false
    type = text
  }
  column "description" {
    null = false
    type = text
  }
  column "is_default" {
    null = false
    type = bool
  }
  primary_key {
    columns = [column.id]
  }
  index "branch_pk_field_branch_id" {
    columns = [column.branch_id]
  }
  index "branch_pk_composed" {
    unique  = true
    columns = [column.branch_id]
  }
}
table "configurations" {
  schema = schema.main
  column "id" {
    null = false
    type = text
  }
  column "branch_id" {
    null = false
    type = integer
  }
  column "component_id" {
    null = false
    type = text
  }
  column "config_id" {
    null = false
    type = text
  }
  column "name" {
    null = false
    type = text
  }
  column "description" {
    null = false
    type = text
  }
  column "is_disabled" {
    null    = false
    type    = bool
    default = false
  }
  column "content" {
    null = false
    type = json
  }
  column "configuration_parent" {
    null = false
    type = text
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "configurations_branches_parent" {
    columns     = [column.configuration_parent]
    ref_columns = [table.branches.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "configuration_pk_field_branch_id" {
    columns = [column.branch_id]
  }
  index "configuration_pk_field_component_id" {
    columns = [column.component_id]
  }
  index "configuration_pk_field_config_id" {
    columns = [column.config_id]
  }
  index "configuration_pk_composed" {
    unique  = true
    columns = [column.branch_id, column.component_id, column.config_id]
  }
}
table "configuration_rows" {
  schema = schema.main
  column "id" {
    null = false
    type = text
  }
  column "branch_id" {
    null = false
    type = integer
  }
  column "component_id" {
    null = false
    type = text
  }
  column "config_id" {
    null = false
    type = text
  }
  column "row_id" {
    null = false
    type = text
  }
  column "name" {
    null = false
    type = text
  }
  column "description" {
    null = false
    type = text
  }
  column "is_disabled" {
    null    = false
    type    = bool
    default = false
  }
  column "content" {
    null = false
    type = json
  }
  column "configuration_row_parent" {
    null = false
    type = text
  }
  primary_key {
    columns = [column.id]
  }
  foreign_key "configuration_rows_configurations_parent" {
    columns     = [column.configuration_row_parent]
    ref_columns = [table.configurations.column.id]
    on_update   = NO_ACTION
    on_delete   = CASCADE
  }
  index "configurationrow_pk_field_branch_id" {
    columns = [column.branch_id]
  }
  index "configurationrow_pk_field_component_id" {
    columns = [column.component_id]
  }
  index "configurationrow_pk_field_config_id" {
    columns = [column.config_id]
  }
  index "configurationrow_pk_field_row_id" {
    columns = [column.row_id]
  }
  index "configurationrow_pk_composed" {
    unique  = true
    columns = [column.branch_id, column.component_id, column.config_id, column.row_id]
  }
}
schema "main" {
}
