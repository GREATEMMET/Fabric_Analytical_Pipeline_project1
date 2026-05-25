[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"crm_cust_info csv" = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, PreserveOrder = null]),
  #"Navigation 1" = Source{[workspaceId = "0d2db1d8-2bee-419b-857d-cb6c72a5249c"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "d2570465-df8b-4df5-9064-8f6f34c385d7"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "source_crm"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "crm_cust_info.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 5", [Delimiter = ",", Columns = 7, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"cst_id", Int64.Type}, {"cst_key", type text}, {"cst_firstname", type text}, {"cst_lastname", type text}, {"cst_marital_status", type text}, {"cst_gndr", type text}, {"cst_create_date", type date}}),
  #"Removed duplicates" = Table.Distinct(#"Changed column type"),
  #"Trimmed text" = Table.TransformColumns(#"Removed duplicates", {{"cst_gndr", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"cst_firstname", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"cst_lastname", each Text.Trim(_), type nullable text}}),
  #"Uppercased text" = Table.TransformColumns(#"Trimmed text 2", {{"cst_marital_status", each Text.Upper(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Uppercased text", "M", "Married", Replacer.ReplaceText, {"cst_marital_status"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "S", "Single", Replacer.ReplaceText, {"cst_marital_status"}),
  #"Uppercased text 1" = Table.TransformColumns(#"Replaced value 1", {{"cst_gndr", each Text.Upper(_), type nullable text}}),
  #"Replaced value 2" = Table.ReplaceValue(#"Uppercased text 1", "M", "Male", Replacer.ReplaceText, {"cst_gndr"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "F", "Female", Replacer.ReplaceText, {"cst_gndr"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "", "n/a", Replacer.ReplaceValue, {"cst_gndr"})
in
  #"Replaced value 4";
shared #"crm_sales_details csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "0d2db1d8-2bee-419b-857d-cb6c72a5249c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d2570465-df8b-4df5-9064-8f6f34c385d7"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "source_crm"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "crm_sales_details.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"sls_ord_num", type text}, {"sls_prd_key", type text}, {"sls_cust_id", Int64.Type}, {"sls_order_dt", Int64.Type}, {"sls_ship_dt", Int64.Type}, {"sls_due_dt", Int64.Type}, {"sls_sales", Int64.Type}, {"sls_quantity", Int64.Type}, {"sls_price", Int64.Type}}),
  #"Calculated absolute value" = Table.TransformColumns(#"Changed column type", {{"sls_price", each Number.Abs(_), type nullable number}})
in
  #"Calculated absolute value";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Merge_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Merge = let
  Source = Table.NestedJoin(#"crm_cust_info csv", {"cst_id"}, #"crm_sales_details csv", {"sls_cust_id"}, "crm_sales_details csv", JoinKind.LeftOuter),
  #"Expanded crm_sales_details csv" = Table.ExpandTableColumn(Source, "crm_sales_details csv", {"sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt", "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"}, {"crm_sales_details csv.sls_ord_num", "crm_sales_details csv.sls_prd_key", "crm_sales_details csv.sls_cust_id", "crm_sales_details csv.sls_order_dt", "crm_sales_details csv.sls_ship_dt", "crm_sales_details csv.sls_due_dt", "crm_sales_details csv.sls_sales", "crm_sales_details csv.sls_quantity", "crm_sales_details csv.sls_price"})
in
  #"Expanded crm_sales_details csv";
shared Merge_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "0d2db1d8-2bee-419b-857d-cb6c72a5249c"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "d2570465-df8b-4df5-9064-8f6f34c385d7"]}[Data],
  TableNavigation = Navigation_2{[Id = "Merge_dataflow", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
