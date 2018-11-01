#!/usr/local/bin/python
# -*- coding: utf-8 -*-
'''da_client
'''

import sys, os
import sqlite3
from googleapiclient.http import MediaFileUpload

from abstract_client import CACHE_FOLDERIDS
from abstract_client import MAX_ACT_LEN, MAX_KEY_LEN, MAX_PATH_LEN
from abstract_client import FOLDER_TYPE
from abstract_client import AbstractClient

class DAClient(AbstractClient):
  srv_name = 'drive'
  srv_version = 'v2'

  def __init__(self, basedir=None, **kwargs):
    self.folderIds = None # will be set in DAClient.build()
    super(DAClient, self).__init__(basedir, **kwargs)
    self.printFields = ['modifiedDate', 'title', 'id', 'mimeType']
    self.printCallback = self.defaultPrintCallback

  def build(self, http):
    '''will be called by AbstractClient.__init__()'''
    super(DAClient, self).build(http)
    self.folderIds = os.path.join(self.basedir,
      CACHE_FOLDERIDS % (self.clientId, self.safe_fname(self.oa2act)))
    self.initializeCacheFolderIds()
    return self.service

  def setPrintFields(self, pfields):
    self.printFields = pfields

  def setPrintCallback(self, pcallback):
    self.printCallback = pcallback

  def defaultPrintCallback(self, e):
    for f in e['items']:
      for fld in self.printFields:
        print f[fld],
      print
    print 'len: %d, hasNext: %s' % (len(e['items']), 'nextPageToken' in e)

  def execQuery(self, q, repeattoken=False, noprint=False, **kwargs):
    '''
    kwargs = {'maxResults': 10} # default maxResults=100
    '''
    result = None
    npt = ''
    while not npt is None:
      if npt != '': kwargs['pageToken'] = npt
      kwargs['supportsTeamDrives'] = True
      kwargs['includeTeamDriveItems'] = True
      e = self.service.files().list(q=q, **kwargs).execute()
      if result is None: result = e
      else: result['items'] += e['items']
      # e does not have 'nextPageToken' key when len(e['items']) <= maxResults
      npt = e.get('nextPageToken')
      if not noprint and not self.printCallback is None: self.printCallback(e)
      if not repeattoken: break
    return result

  def procentry(self, mode, folderId, q, **kwargs):
    result = []
    c = '=' if mode else '!='
    query = "'%s' in parents and mimeType%s'%s'" % (folderId, c, FOLDER_TYPE)
    entries = self.execQuery(query, True, True, **kwargs)
    for entry in entries['items']:
      result += [map(lambda a: entry[a], self.printFields)]
    return result

  def walk_visit(self, folderId, visit, arg,
    depth=[], topdown=True, q=None, **kwargs):
    '''like os.path.walk()'''
    edirs = self.procentry(True, folderId, q, **kwargs)
    efiles = self.procentry(False, folderId, q, **kwargs)
    if not len(depth):
      depth = [('0000-00-00T00:00:00.000Z', '', folderId, None)]
    visit(arg, depth, edirs)
    if topdown: visit(arg, depth, edirs, efiles)
    for ed in edirs:
      self.walk_visit(ed[2], visit, arg, depth + [ed])
    if not topdown: visit(arg, depth, edirs, efiles)

  def walk_iter(self, folderId, topdown=True, q=None, **kwargs):
    '''like os.walk()'''

    def procfolders(epaths, folders):
      for folder in folders:
        id = folder[2]
        edirs = self.procentry(True, id, q, **kwargs)
        efiles = self.procentry(False, id, q, **kwargs)
        nepaths = epaths + [folder]
        if topdown: yield nepaths, edirs, efiles
        for ep, ed, ef in procfolders(nepaths, edirs):
          yield ep, ed, ef
        if not topdown: yield nepaths, edirs, efiles

    epaths = []
    folders = [('0000-00-00T00:00:00.000Z', '', folderId, None)]
    for ep, ed, ef in procfolders(epaths, folders):
      yield ep, ed, ef

  def initializeCacheFolderIds(self):
    if os.path.exists(self.folderIds):
      cn = sqlite3.connect(self.folderIds)
      try:
        cn.execute('select * from oauth2acts where id=1;')
        checkVersion = True
      except (Exception, ), e:
        checkVersion = False
      cn.close()
      if not checkVersion:
        raise Exception('%s is old version, please delete it' % self.folderIds)
      return
    cn = sqlite3.connect(self.folderIds)
    cn.execute('''\
create table oauth2acts (
 id integer primary key autoincrement,
 oauth2act varchar(%d) unique not null,
 credentials text default '');''' % (
      MAX_ACT_LEN))
    cn.execute('''\
create unique index oauth2acts_idx_oauth2act on oauth2acts (oauth2act);''')
    cn.execute('''\
insert into oauth2acts (oauth2act, credentials) values (?, ?);''', (
      self.oa2act, ''))
    cn.execute('''\
create table folderIds (
 key varchar(%d) primary key not null,
 val varchar(%d) unique not null,
 act integer default 1,
 fol integer default 1,
 flg integer default 0);''' % (
      MAX_KEY_LEN, MAX_PATH_LEN))
    cn.execute('''\
create unique index folderIds_idx_val on folderIds (val);''')
    cn.execute('''\
create index folderIds_idx_act on folderIds (act);''')
    cn.execute('''\
insert into folderIds (key, val) values ('root', '/');''')
    cn.commit()
    cn.close()

  def makeDirs(self, folder):
    '''folder must start with '/'
    returns id string, folder string
    '''
    return self.prepare_folder(folder)

  def createFolder(self, name, parentId='root'):
    '''name must *NOT* contain '/'
    returns id string, folder Object
    '''
    body = {'title': name, 'mimeType': FOLDER_TYPE, 'description': name}
    body['parents'] = [{'id': parentId}]
    folder = self.service.files().insert(body=body, supportsTeamDrives=True).execute()
    print ">> Directory created\n"
    return (folder['id'], folder)

  def uploadFile(self, path, filename, parentId, fileId=None):
    # body = {'title': filename, 'mimeType': mimetype, 'description': filename}
    body = {'title': filename, 'description': filename}
    body['parents'] = [{'id': parentId}]
    filepath = os.path.join(path, filename)
    # mbody = MediaFileUpload(filepath, mimetype=mimetype, resumable=True)
    mbody = MediaFileUpload(filepath, resumable=True)
    if mbody._mimetype is None: mbody._mimetype = 'application/octet-stream'
    if fileId is None:
      fileObj = self.service.files().insert(
        body=body, media_body=mbody, supportsTeamDrives=True).execute()
    else:
      fileObj = self.service.files().update(
        fileId=fileId, body=body, media_body=mbody, supportsTeamDrives=True).execute()
    print ">> Uploaded %s" % filename
    return (fileObj['id'], fileObj)

  def raise_if_folder_ignored(self, folder, root='root'):
    q = folder.replace('\\', '/')
    if len(q) > MAX_PATH_LEN:
      raise Exception('folder length is too long > %s' % MAX_PATH_LEN)
    if q[0] != '/':
      raise Exception('folder does not start with / [%s]' % folder)
    if q[-1] == '/' or not len(q):  # root or endswith '/'
      return (root, '/')
    if os.path.exists(os.path.join(q, 'NO_UPLOAD')):
      raise Exception('folder %s is marked as NO_UPLOAD' % q)
    if os.path.split(folder)[-1] == '@eaDir':
      raise Exception('not uploading @eaDir')

  def prepare_folder(self, folder, root='root'):
    q = folder.replace('\\', '/')
    self.raise_if_folder_ignored(folder, root)
    cn = sqlite3.connect(self.folderIds)
    cn.row_factory = sqlite3.Row
    cur = cn.cursor()
    cur.execute('''\
select key from folderIds where val=? and act=? and fol=? and flg=?;''', (
      q.decode('utf-8'), 1, 1, 0))
    row = cur.fetchone()
    cur.close()
    cn.close()
    if row is None:
      parent, p = os.path.split(q)
      parentId, r = self.prepare_folder(parent, root=root)
      query = "'%s' in parents and title='%s' and mimeType='%s' %s" % (
        parentId, p.decode('utf-8'), FOLDER_TYPE, 'and explicitlyTrashed=False')
      entries = self.execQuery(query, True, True, **{'maxResults': 2})
      if not len(entries['items']):
        print ">> Creating new directory %s\n" % q
        folderId, folderObj = self.createFolder(p, parentId)
      else:
        folderId = entries['items'][0]['id']
        if len(entries['items']) > 1:
          sys.stderr.write('duplicated folder [%s]\a\n' % q)
      cn = sqlite3.connect(self.folderIds)
      cn.execute('''\
insert into folderIds (key, val, act, fol, flg) values (?, ?, ?, ?, ?);''', (
        folderId, q.decode('utf-8'), 1, 1, 0))
      cn.commit()
      cn.close()
    else:
      folderId = row['key']
    return (folderId, q)

  def process_file(self, path, filename, parentId, parent):
    cn = sqlite3.connect(self.folderIds)
    cn.row_factory = sqlite3.Row
    cur = cn.cursor()
    cur.execute('''\
select key from folderIds where val=? and act=? and fol=? and flg=?;''', (
      u'%s/%s' % (parent.decode('utf-8'), filename.decode('utf-8')), 1, 0, 0))
    row = cur.fetchone()
    cur.close()
    cn.close()
    if row is None:
      query = u"'%s' in parents and title='%s' and mimeType!='%s' %s" % (
        parentId, filename.decode('utf-8'), FOLDER_TYPE, 'and explicitlyTrashed=False')
      entries = self.execQuery(query, True, True, **{'maxResults': 2})
      if not len(entries['items']):
        print ">> Uploading new file %s %s" % (parent, filename)
        fileId, fileObj = self.uploadFile(path, filename, parentId)
      else:
        print ">> Updating file %s %s" % (parent, filename)
        fileId = entries['items'][0]['id']
        if len(entries['items']) > 1:
          sys.stderr.write('EE duplicated file [%s/%s]\a\n' % (parent, filename))
        fileId, fileObj = self.uploadFile(path, filename, parentId, fileId)
      cn = sqlite3.connect(self.folderIds)
      cn.execute('''\
insert into folderIds (key, val, act, fol, flg) values (?, ?, ?, ?, ?);''', (
        fileId, u'%s/%s' % (parent.decode('utf-8'), filename.decode('utf-8')), 1, 0, 0))
      cn.commit()
      cn.close()
    else:
      # print "file already uploaded: %s" % filename
      fileId = row[0]
      fileObj = None
      #fileId, fileObj = self.uploadFile(path, filename, parentId, row['key'])
    return (fileId, fileObj)

  def recursiveUpload(self, remote, team_drive_id=None):

    basedir = self.basedir
    b = os.path.join(basedir, remote)
    root = 'root' if team_drive_id is None else team_drive_id
    remote_id, q = self.prepare_folder(b[len(basedir):], root=root) # set [0]='/'
    print ">> Processing dir %s" % q

    for path, dirs, files in os.walk(b, topdown=True):
      try:
        p_id, q = self.prepare_folder(path[len(basedir):], root=root) # set [0]='/'
        print ">> Processing dir %s" % q

        dirs_to_exclude = set()
        for d in dirs:
          try:
            self.raise_if_folder_ignored(os.path.join(path, d))
            print '>> Adding subdir %s/%s' % (q, d)  # os.path.join(path, d)
          except Exception, e:
            print ">> Subdir %s/%s is ignored because: %s" % (q, d, str(e))
            dirs_to_exclude.add(d)
        dirs[:] = set(dirs) - dirs_to_exclude
        for f in files:
          # print 'F %s %s' % (q, f) # os.path.join(path, f)
          sys.stdout.write('.')
          try:
            fileId, fileObj = self.process_file(path, f, p_id, q)
            # pprint.pprint((fileId, fileObj))
          except Exception as ex2:
            print str(ex2)
        sys.stdout.write("\n")
      except Exception as ex:
        print str(ex)

  def downloadFile(self, path, filename, parentId, fileId=None, mimetype=None):
    '''
    path: output path
    filename: output filename
    parentId: parentId of file (ignored when parentId=None or fileId is set)
    fileId: fileId to get (if None: search by filename and parentId)
    mimetype: search mimetype (type conversion will be implemented future)
    '''
    from apiclient import errors
    if fileId is None:
      fileInfo = (filename, mimetype)
      q = "title contains '%s'" % filename
      if parentId: q = "%s and '%s' in parents" % (q, parentId)
      if mimetype: q = "%s and mimeType='%s'" % (q, mimetype)
      entries = self.execQuery(q, noprint=True, maxResults=2)
      cnt = len(entries['items'])
      if not cnt:
        sys.stderr.write('not found [%s] mimeType[%s]\n' % fileInfo)
        return (None, None)
      if cnt > 1:
        sys.stderr.write('duplicated [%s] mimeType[%s]\a\n' % fileInfo)
      # pprint.pprint(entries)
      fileObj = entries['items'][0]
      fileId = fileObj['id']
    else:
      try:
        fileObj = self.service.files().get(fileId=fileId).execute()
      except (errors.HttpError, ), e:
        fileObj = None
    if fileObj:
      download_url = fileObj.get('downloadUrl', None)
      if download_url:
        resp, content = self.service._http.request(download_url)
        if resp.status == 200:
          f = open(os.path.join(path, filename), 'wb')
          f.write(content)
          f.close()
        else:
          sys.stderr.write('an error occurred: %s %s\n' % (resp, download_url))
      else:
        sys.stderr.write('not found downloadUrl for fileId: %s\n' % fileId)
    else:
      sys.stderr.write('not found fileId: %s\n' % fileId)
    return (fileObj.get('id', None) if fileObj else None, fileObj)
