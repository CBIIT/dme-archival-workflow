delete from collection_name_mapping;
insert into collection_name_mapping values(1, 'pi_lab','0001', 'John_Doe');
insert into collection_name_mapping values(2, 'project','abc', 'Project_1');
insert into collection_name_mapping values(3, 'project','Flowcell1', 'Project_1');
insert into collection_name_mapping values(4, 'project','Flowcell2', 'Project_2');
insert into collection_name_mapping values(5, 'project','Flowcell3', 'Project_3');
insert into collection_name_mapping values(6, 'project','Flowcell4', 'Project_4');
--HiTIF Example
insert into collection_name_mapping values(7, 'PI','Jane', 'John_Doe');
insert into collection_name_mapping values(8, 'User','Jane', 'Jane_Doe');

delete from metadata_mapping;
insert into metadata_mapping values(1, 'abc', 'project', 'projectName', 'Project 123');
insert into metadata_mapping values(2, 'Flowcell1', 'project', 'projectName', 'Project 1');
insert into metadata_mapping values(3, 'Flowcell2', 'project', 'projectName', 'Project 2');
insert into metadata_mapping values(4, 'Flowcell3', 'project', 'projectName', 'Project 3');
insert into metadata_mapping values(5, 'Flowcell4', 'project', 'projectName', 'Project 4');
--HiTIF Example
insert into metadata_mapping values(6, 'John_Doe', 'PI', 'collection_type', 'PI');
insert into metadata_mapping values(7, 'John_Doe', 'PI', 'pi_name', 'John Doe');
insert into metadata_mapping values(8, 'John_Doe', 'PI', 'pi_email', 'john.doe@nih.gov');
insert into metadata_mapping values(9, 'John_Doe', 'PI', 'institute', 'NCI');
insert into metadata_mapping values(10, 'John_Doe', 'PI', 'lab', 'CCR');
insert into metadata_mapping values(11, 'Jane_Doe', 'User', 'collection_type', 'User');
insert into metadata_mapping values(12, 'Jane_Doe', 'User', 'name', 'Jane Doe');
insert into metadata_mapping values(13, 'Jane_Doe', 'User', 'email', 'jane.doe@nih.gov');
insert into metadata_mapping values(14, 'Jane_Doe', 'User', 'branch', 'CCR');
insert into metadata_mapping values(15, 'Jane_Doe', 'User', 'comment', 'Test');
--CMM Example
insert into metadata_mapping values(16, '0022', 'PI', 'collection_type', 'PI_Lab');
insert into metadata_mapping values(17, '0022', 'PI', 'pi_name', 'John Doe');
insert into metadata_mapping values(18, '0022', 'PI', 'affiliation', 'TSRI');
insert into metadata_mapping values(19, 'HIV_Trimer', 'Project', 'collection_type', 'Project');
insert into metadata_mapping values(20, 'HIV_Trimer', 'Project', 'project_name', 'Sample project name');
insert into metadata_mapping values(21, 'HIV_Trimer', 'Project', 'start_date', '4/1/2018');
insert into metadata_mapping values(22, 'HIV_Trimer', 'Project', 'description', 'Some description');
--insert into metadata_mapping values(23, 'HIV_Trimer', 'Project', 'publications', 'Placeholder for publication');
insert into metadata_mapping values(24, '0005', 'PI', 'collection_type', 'PI_Lab');
insert into metadata_mapping values(25, '0005', 'PI', 'pi_name', 'Jane Doe');
insert into metadata_mapping values(26, '0005', 'PI', 'affiliation', 'CBIIT');
insert into metadata_mapping values(27, '0005', 'Project', 'collection_type', 'Project');
insert into metadata_mapping values(28, '0005', 'Project', 'project_name', 'Sample project name 2');
insert into metadata_mapping values(29, '0005', 'Project', 'start_date', '2/22/2019');
insert into metadata_mapping values(30, '0005', 'Project', 'description', 'Some description');
--Example permission and bookmark
insert ignore into permission_bookmark_info values (1, '/TEST_Archive', 'jdoe', 'READ', 'Y', 'N', null);