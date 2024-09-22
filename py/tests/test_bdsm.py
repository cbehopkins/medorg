# from pathlib import Path
# from bkp_p.bdsm import Bdsm
# from bkp_p.bkp_xml import BkpFile


# def test_update_db_file_entry_one_file(tmp_path):
#     session = Bdsm(tmp_path)
#     dummy_id = "my_id"
#     session.init_dest(dummy_id)
#     ftb = session.files_to_backup(dummy_id)
#     assert not ftb
#     session.update_file_entry(
#         contents=BkpFile(
#             name="my_file",
#             file_path=Path("here/there"),
#             size=10,
#             timestamp=100,
#         ),
#         path=Path("here/there"),
#     )
#     ftb = session.files_to_backup(dummy_id)
#     assert len(ftb) == 1


# def test_update_db_file_entry_one_file_already_backed_up(tmp_path):
#     session = Bdsm(tmp_path)
#     dummy_id = "my_id"
#     # When we have a file that already has the backup desst set to the current one
#     session.update_file_entry(
#         contents=BkpFile(
#             name="my_file",
#             file_path=Path("here/there"),
#             size=10,
#             timestamp=100,
#             bkp_dests={dummy_id},
#         ),
#         path=Path("here/there"),
#     )
#     # Then it doesn't appear in the list to backup
#     ftb = session.files_to_backup(dummy_id)
#     assert len(ftb) == 0


# def test_update_db_file_entry_multiple_files(tmp_path):
#     dummy_files = [
#         BkpFile(
#             name="my_file",
#             file_path=Path("here/there/my_file"),
#             size=10,
#             timestamp=100,
#         ),
#         BkpFile(
#             name="my_other_file",
#             file_path=Path("here/there/my_other_file"),
#             size=10,
#             timestamp=100,
#         ),
#     ]
#     session = Bdsm(tmp_path)
#     dummy_id = "my_id"
#     entries = []
#     for file in dummy_files:
#         entries.append(
#             session.update_file_entry(
#                 contents=file,
#                 path=file.file_path,
#             )
#         )
#     ftb = session.files_to_backup(dummy_id)
#     assert len(ftb) == len(dummy_files)
#     assert len(session.modified_files) == 0
#     for entry in entries:
#         entry.modified = 1
#     assert len(entries) == len(dummy_files)
