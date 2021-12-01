
            SELECT id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext$teampartners, location, updated, headou, ouext$syncuser FROM tbl_ou 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        