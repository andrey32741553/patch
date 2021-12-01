
            SELECT id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role$linktoprocess, title FROM tbl_team 
            ORDER BY id ASC OFFSET {offset} LIMIT {limit}
        