
            SELECT id, removal_date, removed, description, status, activecopy_id, code, color, folder, pos, parent, title_en, title_client, title FROM tbl_servicetime 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        