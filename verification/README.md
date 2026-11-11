独立验收说明

冻结提交：https://github.com/9873199/aiohttp-streaming-ws-limits/commit/566d211af83387106c03640db17d47ef9cf4f79a
实际解释器：3.14.3 (tags/v3.14.3:323c59a, Feb  3 2026, 16:04:56) [MSC v.1944 64 bit (AMD64)]

从冻结提交恢复完整源码，实际Python/依赖版本见environment-reviewed.json。replay/r1记录Cython生成全部扩展、准备的本机编译工具链安装与实际.pyd导入路径；初始llhttp为官方固定commit archive，父gitlink/provenance/SHA匹配，不能把无.git归档假称clone。acceptance-r1含parser/writer/新增inflate回归170过11跳过和独立22项全过；truncation-r1四过，release-r1两过。后两组support websocket_checks.py只是同一独立frame/reader夹具，没有插入产品安全逻辑。原生语料测试模块本身参数化两种实现，默认模式和NO_EXTENSIONS=1各22过不能加成44个不同用例。原轨最后五文件181/4与179/6有效，功能套件264/281/284只有默认模式命令；不将最后答复宽泛句作为额外双模式测试证据。scope-and-provenance-r1另存初始148/11基线和可观察测试摘要，不另附原轨或stream。recording是独立验证控制台，连续75秒四命令均0。Zig系统缓存偏差单列execution-cache-audit-r1，1651新文件73704804字节已同SHA迁回cache，旧缓存保留；不得改原轨消除该历史记录。源树快照为精确冻结字节，Windows换行差异可能扩大Git树差异，不把换行变动当模型功能编辑；题面逐字门禁不做任何归一化。

每组目录内的 *-command.json 保留真实参数、工作目录、PYTHONPATH、退出码和原始日志位置；test-results.json 与 JUnit XML 保留完整成绩。跨机器运行时把这些绝对路径映射到同一冻结 checkout 和自己的缓存目录，所有 --basetemp、XML 和构建输出均放在缓存里。依赖版本见 environment-reviewed.json。
这些材料在模型单轮结束后生成，与原轨内测试分开，不代表项目方质检通过。原轨和视频只放各自的飞书附件。


冻结产物提交：https://github.com/9873199/aiohttp-streaming-ws-limits/commit/566d211af83387106c03640db17d47ef9cf4f79a
这些独立验收文件在模型首轮结束后生成，不是模型原轨中的测试。
