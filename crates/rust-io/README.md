# rust-io

> Lingua Seeker 的 PyO3 门面 crate，将 `net-io`、`files-io` 和 `io-py` 注册为 `rust_io` Python 模块的子模块。

## 概览

`rust-io` 是唯一构建为 `cdylib`（`.so`/`.pyd`）的 crate，通过 `import rust_io` 加载。它本身不包含业务逻辑，仅负责：

1. 创建 `rust_io.net` 子模块，注册 `net-io` 的所有 `#[pyfunction]`
2. 创建 `rust_io.files` 子模块，注册 `files-io` 的所有 `#[pyfunction]` 和 `#[pyclass]`
3. 创建 `rust_io.io` 子模块，注册 `io-py` 的通用异步 I/O 函数
4. 通过 `sys.modules` 注册子模块，使其可通过 `import rust_io.net` / `rust_io.files` / `rust_io.io` 直接访问

## 公开 API

`rust-io` 本身不定义业务 API，完整 API 参考见各子 crate：

| Python 模块 | 来源 crate | 说明 |
|-------------|-----------|------|
| `rust_io.net` | [net-io](../net-io/README.md) | 15 个文献数据源、MinerU API、Web 爬取 |
| `rust_io.files` | [files-io](../files-io/README.md) | File 类、批量操作、去重、工具函数 |
| `rust_io.io` | [io-core](../io-core/README.md) | 通用异步 I/O：本地/HTTP/S3、多路复用、流式传输 |

### `rust_io.net` 注册的函数

| 函数 | 说明 |
|------|------|
| `fetch_one` | 单数据源文献搜索/下载 |
| `fetch_multi` | 并行多数据源搜索 |
| `scrape_web` | 数据源网页爬取 |
| `scrape_html` | CSS 选择器 HTML 解析 |
| `extract_pdf_links` | 提取 PDF 链接 |
| `download_file` | 文件下载 |
| `mineru_create_task` | MinerU 创建解析任务 |
| `mineru_get_result` | MinerU 查询任务结果 |
| `mineru_batch_submit` | MinerU 批量提交 |
| `mineru_batch_result` | MinerU 批量结果查询 |
| `mineru_create_upload_url` | MinerU 获取上传 URL |
| `mineru_create_batch_upload_urls` | MinerU 批量获取上传 URL |
| `mineru_upload_local_file` | MinerU 上传单个本地文件 |
| `mineru_upload_local_files` | MinerU 上传多个本地文件 |

### `rust_io.files` 注册的函数/类

| 名称 | 类型 | 说明 |
|------|------|------|
| `File` | class | 统一文件操作（本地 + S3） |
| `batch_copy` | function | 批量文件复制 |
| `batch_compress` | function | 批量目录压缩 |
| `batch_copy_async` | function | 异步批量复制 |
| `check_duplicate` | function | 文件去重检查 |
| `batch_hash` | function | 批量 SHA-256 哈希 |
| `compute_sha256` | function | 单文件 SHA-256 |
| `write_file` | function | 写入字节数据 |
| `validate_pdf_magic` | function | PDF 魔数校验 |

### `rust_io.io` 注册的函数

| 函数 | 说明 |
|------|------|
| `read` / `read_range` | 读取整个对象 / 指定字节区间 |
| `write` | 写入 bytes/bytearray/memoryview/str |
| `stat` / `list` / `delete` / `copy` | 元数据、目录列举、删除、复制 |
| `read_many` / `write_many` / `copy_many` | 有界并发批处理，结果按输入顺序返回 |
| `read_any` | 多镜像竞速，返回首个成功结果 |
| `download` / `upload` | 流式下载/上传（不整文件驻留内存） |
| `hash` / `hash_many` | 流式 SHA-256 / rayon 并行批哈希 |

## 架构

```
rust-io/src/
└── lib.rs    # #[pymodule] rust_io — 挂载 net、files、io 子模块
```

### 子模块注册机制

`net-io` 和 `files-io` 各自用 `#[pymodule(submodule)]` 声明 Python 子模块并导出自己的 API，`rust-io` 只负责挂载，不再逐个重复注册函数：

```rust
#[pymodule]
fn rust_io(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let net = pyo3::wrap_pymodule!(net_io::py::net)(py);
    register_submodule(m, "net", "rust_io.net", net.bind(py))?;
    let files = pyo3::wrap_pymodule!(files_io::py::files)(py);
    register_submodule(m, "files", "rust_io.files", files.bind(py))?;
    let io = pyo3::wrap_pymodule!(io_py::io)(py);
    register_submodule(m, "io", "rust_io.io", io.bind(py))?;
    Ok(())
}
```

`register_submodule` 完成三步操作：
1. `parent.add(name, submodule)` — 挂载为父模块属性
2. 设置 `submodule.__name__ = "rust_io.net"`（完整限定名）
3. `sys.modules["rust_io.net"] = submodule` — 使 `import rust_io.net` 可用

新增函数只需在所属 crate 的 `#[pymodule]` 中导出，门面 crate 无需改动。

## 使用示例

```python
# 通过 rust_io 门面访问所有功能
import rust_io

# 或直接导入子模块
import rust_io.net as net_io
import rust_io.files as files_io

# 文献搜索
result = await net_io.fetch_one("crossref", "search", {"query": "CRISPR"})

# 文件操作
f = files_io.File("/tmp/output.txt")
f.write("hello")
print(f.read(as_text=True))

# 通用异步 I/O（本地/HTTP/S3 统一 URI）
import rust_io.io as io_io
await io_io.write("/tmp/report.bin", b"payload")
results = await io_io.read_many(
    ["/tmp/a.bin", "s3://bucket/b.bin", "https://example.com/c.bin"],
    limit=3,
)
await io_io.download("https://example.com/paper.pdf", "/tmp/paper.pdf")
```

## 构建

```bash
# 从仓库根目录构建（maturin 会自动解析 Cargo.toml 中的 path 依赖）
uv run maturin develop --release -m crates/rust-io/Cargo.toml
```

构建产物为 `rust_io.so`（Linux/macOS）或 `rust_io.pyd`（Windows），包含 `net-io`、`files-io` 和 `io-core` 的全部代码（静态链接）。

## 依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| `pyo3` | `0.30` | Python 绑定（extension-module） |
| `net-io` | path | HTTP/Web I/O crate（提供 `rust_io.net`） |
| `files-io` | path | 文件 I/O crate（提供 `rust_io.files`） |
| `io-py` | path | 通用 I/O 绑定 crate（提供 `rust_io.io`） |
