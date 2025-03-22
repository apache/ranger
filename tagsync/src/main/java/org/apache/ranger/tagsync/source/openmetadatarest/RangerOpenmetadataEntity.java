package org.apache.ranger.tagsync.source.openmetadatarest;

import java.util.UUID;

import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openmetadata.client.model.Column;
import org.openmetadata.client.model.Table;
import org.openmetadata.client.model.EntityReference;

@Data
@Getter
@Setter
@Builder
public class RangerOpenmetadataEntity {
    private static final Logger LOG = LoggerFactory.getLogger(RangerOpenmetadataEntity.class);
    private  String type;
    private  UUID id;
    private  Table tableEntityObject;
    private  Column columnEntityObject;

    public RangerOpenmetadataEntity(String type, UUID id, Table tableEntityObject, Column columnEntityObject) {
        this.type = type;
        this.id = id;
        this.tableEntityObject = tableEntityObject;
        this.columnEntityObject = columnEntityObject;
    }
    @Override
    public String toString() {
        StringBuilder sb_table = new StringBuilder();
        StringBuilder sb_column = new StringBuilder();
        StringBuilder sb = new StringBuilder();
        if (tableEntityObject != null){
            sb_table.append("{typeName=").append(type);
            sb_table.append(", id=");
            sb_table.append(", attributes={");
            sb_table.append(" name=").append("name").append(", value=").append(tableEntityObject.getName());
            // following string will be in the format: database_schema_name.table_name
            sb_table.append(" name=").append("qualifiedName").append(", value=").append(tableEntityObject.getDatabaseSchema().getName() + "." + tableEntityObject.getName());
            if (!tableEntityObject.getOwners().isEmpty()){
                for (EntityReference owner : tableEntityObject.getOwners()){
                    sb_table.append(" name=").append("owner").append(", value=").append(owner.getName());
                }
            }
            else{
                sb_table.append(" name=").append("owner").append(", value= ");
            }
            if (tableEntityObject.getUpdatedAt() != null){
                sb_table.append(" name=").append("createTime").append(", value=").append(tableEntityObject.getUpdatedAt());
            }
            else{
                sb_table.append(" name=").append("createTime").append(", value= ");
            }
            if (tableEntityObject.getDescription() != null){
                sb_table.append(" name=").append("description").append(", value=").append(tableEntityObject.getDescription());
            }
            else{
                sb_table.append(" name=").append("description").append(", value= ");
            }
            sb_table.append("}");

            return sb_table.toString();
        }
        else if (columnEntityObject != null){
            sb_column.append("{typeName=").append(type);
            sb_column.append(", id= ");
            sb_column.append(", attributes={");
            sb_column.append(" name=").append("name").append(", value=").append(columnEntityObject.getName());
            if (!columnEntityObject.getFullyQualifiedName().isEmpty()){
                String columnFullyQualifiedName = columnEntityObject.getFullyQualifiedName();
                String columnschemaNameSubString = columnFullyQualifiedName.split("\\.")[2];
                String columnTableNameSubString = columnFullyQualifiedName.split("\\.")[3];
                String columnQualifiedName = columnschemaNameSubString + "." + columnTableNameSubString + "." + columnEntityObject.getName();
                sb_column.append(" name=").append("qualifiedName").append(", value= ").append(columnQualifiedName);
                sb_column.append(" name=").append("owner").append(", value= ");
                sb_column.append(" name=").append("createTime").append(", value= ");
                if (columnEntityObject.getDescription() != null){
                    sb_column.append(" name=").append("description").append(", value=").append(columnEntityObject.getDescription());
                }
                else{
                    sb_column.append(" name=").append("description").append(", value= ");
                }
                sb_column.append("}");
            } else{
                System.out.println("null qualifiedName received for column type. Expecting a qualifiedName from column"+ columnEntityObject.getName());
            }
            return sb_column.toString();           
        }
        else {
            LOG.error("Neither Table Nor Column Entity Objects are valid. Returning Null String. This causes the map to fail!!");
            return sb.toString();
        }
    }
}
