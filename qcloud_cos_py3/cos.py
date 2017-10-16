import os
import aiohttp
import time
import random
from aiohttp import MultipartWriter
from aiohttp.hdrs import CONTENT_DISPOSITION, CONTENT_TYPE
from aiohttp.payload import StringPayload, BytesPayload
from collections import namedtuple
import requests
from io import BytesIO

from .cos_auth import CosAuth


CosConfig = namedtuple(
    'CosConfig',
    ['app_id', 'secret_id', 'secret_key', 'region', 'bucket']
)

MAX_RETRY = 3


class MyWriter(MultipartWriter):
    """
    aiohttp 的 HTTP header 中，boundary 是带引号的，
    但 COS 不支持带引号的 boundary，只能重写writer，把引号删掉
    """

    def __init__(self, subtype='mixed', boundary=None):
        super().__init__(subtype=subtype, boundary=boundary)
        self._content_type = self._content_type.replace('"', '')

    def append_payload(self, payload):
        """Adds a new body part to multipart writer."""
        if payload.content_type == 'application/octet-stream':
            payload.headers[CONTENT_TYPE] = payload.content_type

        # render headers
        headers = ''.join(
            [k + ': ' + v + '\r\n' for k, v in payload.headers.items()]
        ).encode('utf-8') + b'\r\n'

        self._parts.append((payload, headers, '', ''))


class CosBucket(object):

    def __init__(self, app_id, secret_id, secret_key, bucket_name, region='sh'):
        self.config = CosConfig(app_id, secret_id, secret_key, region, bucket_name)
        self.signer = CosAuth(self.config)
        self.headers = {'Content-Type': 'application/json'}

    def _format_url(self, url_pattern, **extra):
        url_pattern = "http://{region}.file.myqcloud.com" + url_pattern
        return url_pattern.format(**self.config._asdict(), **extra)

    def _req(self, method, url, *args, **kwargs):
        assert method in ('get', 'post')
        send_req = getattr(requests, method)
        res = {}
        for _ in range(MAX_RETRY):
            try:
                res = send_req(url, *args, **kwargs).json()
            except:
                continue
            code = res['code']
            # Operating too fast or
            # Writing too fast on a single dir
            if code in (-71, -143):
                time.sleep(random.randint(1, 3))
                continue
            else:
                return res
        else:
            raise Exception('API request failed when %s %s: %s'
                            % (method, url, res))

    def create_folder(self, dir_name, *, biz_attr=''):
        """
        `创建目录 <https://www.qcloud.com/document/product/436/6061>`_

        :param dir_name: 目录名
        :param biz_attr: 业务属性（可选）
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            "/files/v2/{app_id}/{bucket}/{dir_name}/",
            dir_name=dir_name
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return self._req(
            'post', url, json={'op': 'create', 'biz_attr': biz_attr},
            headers=headers
        )

    def list_folder(self, dir_name, *, prefix=None, num=1000, context=None):
        """
        `列出目录 <https://www.qcloud.com/document/product/436/6062>`_

        :param dir_name: 文件夹名称
        :param prefix: 前缀
        :param num: 查询的文件的数量，最大支持1000，默认查询数量为1000
        :param context: 起始位置。将上次查询结果的context的字段传入，可实现翻页

          注意：如果在进行列表操作的目录是真实目录而非虚拟目录
          (上传文件路径中带有斜线会认为是虚拟目录),
          实际列出的文件数量会是 num - 1

        """
        url = self._format_url("/files/v2/{app_id}/{bucket}/")
        if dir_name:
            url += str(dir_name) + "/"
        if prefix:
            url += str(prefix)

        url += "?op=list&num=" + str(num)
        if context is not None:
            url += '&context=' + str(context)

        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return self._req('get', url, headers=headers)

    def stat_folder(self, dir_name):
        """
        `查询目录属性 <https://www.qcloud.com/document/product/436/6063>`_

        :param dir_name: 目录路径
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            "/files/v2/{app_id}/{bucket}/{dir_name}/?op=stat",
            dir_name=dir_name
        )
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return self._req('get', url, headers=headers)

    def delete_folder(self, dir_name):
        """
        `删除目录 <https://www.qcloud.com/document/product/436/6064>`_

        :param dir_name: 目录路径

        注意:
            * 虚拟目录无法删除，只能删除显式创建的目录
            * 若显式目录中有文件存在，仍可删除该目录，但文件仍然存在于虚拟目录中
        """
        dir_name = dir_name.strip('/')
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/{dir_name}/',
            dir_name=dir_name
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_once(
                self.config.bucket, dir_name + '/'
            )
        }
        return self._req('post', url, json={'op': 'delete'}, headers=headers)

    def upload_file(self, file_stream, upload_filename, *, dir_name='',
                    biz_attr='', replace=True, mime='application/octet-stream'):
        """
        `简单上传文件 <https://www.qcloud.com/document/product/436/6066>`_

        :param file_stream: 类文件对象
        :param upload_filename: 文件名称
        :param dir_name: 目录名称（可选）
        :param biz_attr: 业务属性（可选）
        :param replace: 是否覆盖（可选）
        :param mime: 文件类型，默认为 application/octet-stream (可选)
        """
        insert = '0' if replace else '1'
        url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            url += '/' + dir_name
        url += '/' + upload_filename
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return self._req(
            'post', url,
            data={'op': 'upload', 'biz_attr': biz_attr, 'insertOnly': insert},
            files={'filecontent': ('', file_stream, mime)},
            headers=headers
        )

    async def async_upload_file(self, file_stream, upload_filename, *,
                                dir_name="", biz_attr='', replace=True,
                                mime='application/octet-stream'):
        """
        异步上传文件 (使用简单上传文件接口)

        :param file_stream: 类文件对象
        :param upload_filename: 文件名称
        :param dir_name: 目录名称（可选）
        :param biz_attr: 业务属性（可选）
        :param replace: 是否覆盖（可选）
        :param mime: 文件类型，默认为 application/octet-stream (可选)
        """
        TIMEOUT = 6
        insert = '0' if replace else '1'
        dir_name = dir_name.strip('/')
        url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            url += '/' + dir_name
        url += '/' + upload_filename
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        pl_op = StringPayload('upload')
        pl_op.set_content_disposition('form-data', name='op')
        pl_bz = StringPayload(biz_attr)
        pl_bz.set_content_disposition('form-data', name='biz_attr')
        pl_ir = StringPayload(insert)
        pl_ir.set_content_disposition('form-data', name='insertOnly')
        pl_fc = BytesPayload(file_stream.read())
        pl_fc.set_content_disposition('form-data', name='filecontent', filename='')
        pl_fc._headers[CONTENT_DISPOSITION] = 'form-data; name="filecontent"; filename=""'
        with MyWriter('form-data') as writer:
            writer.append(pl_op)
            writer.append(pl_bz)
            writer.append(pl_ir)
            writer.append(pl_fc)

        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with session.post(url, data=writer, headers=headers,
                                    timeout=TIMEOUT) as resp:
                return await resp.json()

    def _upload_slice_control(self, file_size, slice_size, biz_attr, replace):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_init',
            'filesize': str(file_size),
            'slice_size': str(slice_size),
            'biz_attr': biz_attr,
            'insertOnly': '0' if replace else '1',
        }
        r = self._req('post', self.url, files=data, headers=headers)
        return r['data']['session']

    def _upload_slice_data(self, filecontent, session, offset):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_data',
            'filecontent': filecontent,
            'session': session,
            'offset': str(offset)
        }
        r = self._req('post', self.url, files=data, headers=headers)
        return r['data']

    def _upload_slice_finish(self, session, file_size):
        headers = {
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        data = {
            'op': 'upload_slice_finish',
            'session': session,
            'filesize': str(file_size)
        }
        r = self._req('post', self.url, files=data, headers=headers)
        return r['data']

    def upload_slice_file(self, real_file_path, slice_size, upload_filename, *,
                          offset=0, dir_name='', biz_attr='', replace=True):
        # 此代码由 @a270443177 (https://github.com/a270443177) 贡献
        """
        `分片上传文件 <https://cloud.tencent.com/document/product/436/6067>`_

        :param real_file_path: 文件路径
        :param slice_size: 分片大小，单位为 Byte，有效取值：

          * 524288 (512 KB)
          * 1048576 (1 MB)
          * 2097152 (2 MB)
          * 3145728 (3 MB)

        :param upload_filename: 上传文件名
        :param offset: 起始位移（可选），默认从头开始
        :param dir_name: 上传目录（可选）
        :param biz_attr: 业务属性（可选）
        :param replace: 是否覆盖（可选）

        """
        assert slice_size
        self.url = self._format_url('/files/v2/{app_id}/{bucket}')
        if dir_name is not None:
            self.url += '/' + dir_name
        self.url += '/' + upload_filename
        file_size = os.path.getsize(real_file_path)
        session = self._upload_slice_control(
            file_size=file_size,
            slice_size=slice_size,
            biz_attr=biz_attr,
            replace=replace)

        with open(real_file_path, 'rb') as local_file:
            while offset < file_size:
                file_content = local_file.read(slice_size)
                self._upload_slice_data(filecontent=file_content,
                                        session=session, offset=offset)
                offset += slice_size
            r = self._upload_slice_finish(session=session, file_size=file_size)
        return r

    def upload_file_from_url(self, url, file_name, *, dir_name=''):
        """
        从 url 抓取文件并上传
        （使用简单上传文件接口）

        :param url: 文件url地址
        :param file_name: 文件名称
        :param dir_name: 文件夹名称（可选）
        """
        try:
            r = requests.get(url)
            r.raise_for_status()
        except:
            return {'error': 'download file failed'}
        return self.upload_file(
            BytesIO(r.content), file_name, dir_name=dir_name,
            mime=r.headers.get('content-type')
        )

    def get_file(self, file_path):
        """
        :param file_path: 文件路径
        """
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + file_path
        )
        headers = {
            'Authorization': self.signer.sign_download(
                self.config.bucket, file_path, 30
            )
        }
        return requests.get(url, headers=headers).content

    def move_file(self, source_file_path, dest_file_path):
        """
        `移动文件 <https://cloud.tencent.com/document/product/436/6730>`_

        :param source_file_path: 源文件路径
        :param dest_file_path: 目标路径

        注意:
            目标路径若不以 / 开头，则认为是相对路径
        """
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + source_file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(
                self.config.bucket, source_file_path
            )
        }
        return self._req(
            'post', url,
            data={'op': 'move', 'dest_fileid': dest_file_path,
                  'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')},
            headers=headers
        )

    def copy_file(self, source_file_path, dest_file_path):
        """
        `拷贝文件 <https://www.qcloud.com/document/product/436/7419>`_

        :param source_file_path: 源文件路径
        :param dest_file_path: 目标路径

        注意:
            目标路径若不以 / 开头，则认为是相对路径
        """
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + source_file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(
                self.config.bucket, source_file_path
            )
        }
        return self._req(
            'post', url,
            data={'op': 'copy', 'dest_fileid': dest_file_path,
                  'to_over_write': '0'},
            files={'filecontent': ('', '', 'application/octet-stream')},
            headers=headers
        )

    def delete_file(self, file_path):
        """
        `删除文件 <https://www.qcloud.com/document/product/436/6073>`_

        :param file_path: 文件路径
        """
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(self.config.bucket, file_path)
        }
        return self._req('post', url, json={'op': 'delete'}, headers=headers)

    def stat_file(self, file_path):
        """
        `查询文件属性 <https://www.qcloud.com/document/api/436/6069>`_

        :param file_path: 文件路径
        """
        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/{file_path}?op=stat',
            file_path=file_path
        )
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self.signer.sign_more(self.config.bucket, '', 30)
        }
        return self._req('get', url, headers=headers)

    def update_file_status(self, file_path, *, authority='eInvalid',
                           custom_headers=None):
        """
        `修改文件属性 <https://www.qcloud.com/document/api/436/6072>`_

        :param file_path: 文件路径
        :param authority: 文件权限
        :param custom_headers: 自定义文件头信息
        :type authority: eInvalid / eWRPrivate / eWPrivateRPublic
        """
        assert authority in (
            'eInvalid', # 空权限，此时系统会默认调取 Bucket 权限
            'eWRPrivate', # 私有读写
            'eWPrivateRPublic' # 公有读私有写
        )

        url = self._format_url(
            '/files/v2/{app_id}/{bucket}/' + file_path
        )
        headers = {
            'Authorization': self.signer.sign_once(self.config.bucket, file_path)
        }
        payload = {
            'op': 'update',
            'authority': authority,
            'custom_headers': custom_headers or {}
        }
        return self._req('post', url, json=payload, headers=headers)
