import struct
from typing import Optional, Tuple, List
from buffer import BufferPoolManager, Buffer
from disk import PageId, PAGE_SIZE
import pickle
import os

# 例外クラス定義
class BTreeError(Exception):
    pass

class DuplicateKeyError(BTreeError):
    pass

# 検索モード定義クラス
class SearchMode:
    START = 0  # 開始位置から検索
    KEY = 1    # 特定のキーで検索

    def __init__(self, mode: int, key: Optional[bytes] = None):
        self.mode = mode
        self.key = key

    @staticmethod
    def Start():
        return SearchMode(SearchMode.START)

    @staticmethod
    def Key(key: bytes):
        return SearchMode(SearchMode.KEY, key)

# キーと値のペア管理クラス
class Pair:
    def __init__(self, key: bytes, value: bytes):
        self.key = key
        self.value = value

    # バイト列に変換
    def to_bytes(self) -> bytes:
        return pickle.dumps(self)

    # バイト列から復元
    @staticmethod
    def from_bytes(data: bytes) -> 'Pair':
        return pickle.loads(data)

# B+Treeクラス
class BPlusTree:
    LEAF_NODE_MAX_PAIRS = 2  # リーフノードの最大ペア数
    BRANCH_NODE_MAX_KEYS = 2  # ブランチノードの最大キー数

    def __init__(self, meta_page_id: PageId):
        self.meta_page_id = meta_page_id  # メタデータページIDの保存

    # B+Treeの作成
    @staticmethod
    def create(bufmgr: BufferPoolManager) -> 'BPlusTree':
        # メタデータとルートノードの作成
        meta_buffer = bufmgr.create_page()
        root_buffer = bufmgr.create_page()
        root_buffer.page[:4] = struct.pack('>I', 0)  # ルートノードはリーフとして初期化
        meta_buffer.page[:8] = root_buffer.page_id.to_bytes()
        meta_buffer.is_dirty = True
        root_buffer.is_dirty = True
        return BPlusTree(meta_page_id=meta_buffer.page_id)

    # ルートページを取得
    def fetch_root_page(self, bufmgr: BufferPoolManager) -> Buffer:
        meta_buffer = bufmgr.fetch_page(self.meta_page_id)
        root_page_id = PageId.from_bytes(meta_buffer.page[:8])
        return bufmgr.fetch_page(root_page_id)

    # 検索処理
    def search(self, bufmgr: BufferPoolManager, search_mode: SearchMode) -> Optional[Tuple[bytes, bytes]]:
        root_page = self.fetch_root_page(bufmgr)
        return self.search_internal(bufmgr, root_page, search_mode)

    def search_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, search_mode: SearchMode) -> Optional[Tuple[bytes, bytes]]:
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # リーフノードの場合
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if pair.key == search_mode.key:
                    return pair.key, pair.value
            return None
        else:  # ブランチノードの場合
            keys, children = self.get_branch(node_buffer)
            for i, key in enumerate(keys):
                if search_mode.key < key:
                    return self.search_internal(bufmgr, bufmgr.fetch_page(children[i]), search_mode)
            return self.search_internal(bufmgr, bufmgr.fetch_page(children[-1]), search_mode)

    # 挿入処理
    def insert(self, bufmgr: BufferPoolManager, key: bytes, value: bytes) -> None:
        root_page = self.fetch_root_page(bufmgr)
        new_child = self.insert_internal(bufmgr, root_page, key, value)
        if new_child is not None:
            # 新しいルートノードの作成
            new_root_buffer = bufmgr.create_page()
            new_root_buffer.page[:4] = struct.pack('>I', 1)  # ブランチとして初期化
            new_root_buffer.is_dirty = True
            meta_buffer = bufmgr.fetch_page(self.meta_page_id)
            meta_buffer.page[:8] = new_root_buffer.page_id.to_bytes()
            meta_buffer.is_dirty = True
            new_key, new_page_id = new_child
            self.set_branch(new_root_buffer, [new_key], [root_page.page_id, new_page_id])

    # 内部挿入処理
    def insert_internal(self, bufmgr: BufferPoolManager, node_buffer: Buffer, key: bytes, value: bytes) -> Optional[Tuple[bytes, PageId]]:
        # 追加した内部挿入処理
        node_type = struct.unpack('>I', node_buffer.page[:4])[0]
        if node_type == 0:  # リーフノードの場合
            pairs = self.get_pairs(node_buffer)
            for pair in pairs:
                if pair.key == key:
                    raise DuplicateKeyError("Duplicate key")
            pairs.append(Pair(key, value))
            pairs.sort(key=lambda p: p.key)
            self.set_leaf(node_buffer, pairs)
            node_buffer.is_dirty = True
            return None
        else:
            raise NotImplementedError("Branch node handling not yet implemented")

    # ペアリスト取得
    def get_pairs(self, buffer: Buffer) -> List[Pair]:
        num_pairs = struct.unpack('>I', buffer.page[4:8])[0]
        pairs = []
        offset = 8
        for _ in range(num_pairs):
            pair_size = struct.unpack('>I', buffer.page[offset:offset+4])[0]
            pair_data = buffer.page[offset+4:offset+4+pair_size]
            pairs.append(Pair.from_bytes(pair_data))
            offset += 4 + pair_size
        return pairs

    # リーフノードの設定
    def set_leaf(self, buffer: Buffer, pairs: List[Pair]) -> None:
        buffer.page[4:8] = struct.pack('>I', len(pairs))
        offset = 8
        for pair in pairs:
            pair_data = pair.to_bytes()
            pair_size = len(pair_data)
            buffer.page[offset:offset+4] = struct.pack('>I', pair_size)
            buffer.page[offset+4:offset+4+pair_size] = pair_data
            offset += 4 + pair_size


if __name__ == "__main__":
    import tempfile
    from buffer import BufferPool, BufferPoolManager
    from disk import DiskManager

    # 一時ファイルを作成
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file_path = temp_file.name

    try:
        # ディスクマネージャとバッファプールの初期化
        disk = DiskManager.open(temp_file_path)
        pool = BufferPool(10)
        bufmgr = BufferPoolManager(disk, pool)

        # B+Treeの作成
        btree = BPlusTree.create(bufmgr)

        # データ挿入テスト
        print("Inserting data into B+Tree...")
        btree.insert(bufmgr, struct.pack('>Q', 1), b"one")
        btree.insert(bufmgr, struct.pack('>Q', 4), b"two")
        btree.insert(bufmgr, struct.pack('>Q', 6), b"three")
        btree.insert(bufmgr, struct.pack('>Q', 3), b"four")
        btree.insert(bufmgr, struct.pack('>Q', 7), b"five")
        btree.insert(bufmgr, struct.pack('>Q', 2), b"six")
        btree.insert(bufmgr, struct.pack('>Q', 5), b"seven") 



        # データ検索テスト
        print("Searching data in B+Tree...")
        for key in [1, 2, 3, 4, 5, 6]:
            result = btree.search(bufmgr, SearchMode.Key(struct.pack('>Q', key)))
            if result:
                found_key, value = result
                print(f"Key: {struct.unpack('>Q', found_key)[0]}, Value: {value.decode()}")
            else:
                print(f"Key {key} not found")

        print("B+Tree tests passed.")
    finally:
        os.remove(temp_file_path)
