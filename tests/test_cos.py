import asyncio
import unittest
from qcloud_cos_py3 import CosBucket
import tests.config as conf
from io import BytesIO

cos = CosBucket(
    conf.QCLOUD_APP_ID,
    conf.QCLOUD_SECRET_ID,
    conf.QCLOUD_SECRET_KEY,
    conf.QCLOUD_BUCKET
)


class TestCos(unittest.TestCase):

    def setUp(self):
        self.cos = cos
        res = self.cos.create_folder('cos_test')
        assert res['code'] == 0

    def tearDown(self):
        res = self.cos.delete_folder('cos_test')
        assert res['code'] == 0

    def test_operations(self):
        # 上传文件
        res = self.cos.upload_file(BytesIO(b'Yo come on'), '1.txt',
                                   dir_name='cos_test', mime='text/plain')
        assert res['code'] == 0

        # 获取目录信息
        res = self.cos.stat_folder('/cos_test')
        assert res['data']['ctime']

        # 文件列表
        res = self.cos.list_folder('/cos_test')
        assert len(res['data']['infos']) == 1

        # 文件拷贝
        res = self.cos.copy_file('/cos_test/1.txt', '/cos_test/2.txt')
        assert res['code'] == 0

        res = self.cos.list_folder('/cos_test')
        assert len(res['data']['infos']) == 2

        # 文件移动
        res = self.cos.move_file('/cos_test/2.txt', '/cos_test/3.txt')
        assert res['code'] == 0

        # 修改文件信息
        res = self.cos.update_file_status(
            '/cos_test/3.txt',
            custom_headers={'Content-Type': 'text/javascript'}
        )
        assert res['code'] == 0

        # 获取文件信息
        res = self.cos.stat_file('/cos_test/3.txt')
        assert res['data']['custom_headers']['Content-Type'] == 'text/javascript'

        res = self.cos.list_folder('/cos_test')
        assert len(res['data']['infos']) == 2

        # 文件删除
        res = self.cos.delete_file('/cos_test/1.txt')
        assert res['code'] == 0

        res = self.cos.delete_file('/cos_test/3.txt')
        assert res['code'] == 0

    def test_async_upload(self):

        # 异步并行上传
        async def async_upload(file_stream, file_name):
            return await self.cos.async_upload_file(
                file_stream, file_name, dir_name='/cos_test'
            )

        loop = asyncio.get_event_loop()
        tasks = [async_upload(BytesIO(b'Yo yo'), str(i)) for i in range(3)]
        rs = loop.run_until_complete(asyncio.gather(*tasks))

        names = {r['data']['resource_path'][-2:] for r in rs}
        assert names == {'/0', '/1', '/2'}

        for i in range(3):
            res = self.cos.delete_file('cos_test/{}'.format(i))
            assert res['code'] == 0
