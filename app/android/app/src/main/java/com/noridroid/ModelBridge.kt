package com.noridroid

import android.content.Context
import android.os.Handler
import android.os.Looper
import android.webkit.WebView
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream
import java.net.HttpURLConnection
import java.net.URL
import java.security.MessageDigest
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.zip.ZipInputStream


class ModelBridge(private val appContext: Context) {

    private val mainHandler = Handler(Looper.getMainLooper())
    private var webView: WebView? = null
    private val ioExecutor: ExecutorService = Executors.newCachedThreadPool()

    fun attach(v: WebView) { webView = v }

    companion object {
        /** 模型静态托管根地址 (IPFS 目录, 无 API)。
         *  原网关 api.elake.top 已失效 (SSL 握手失败), 且该网关只服务模型这一件事,
         *  故整体改为静态直链: `<id>.zip` / `<id>-cover.webp` 两个约定路径。 */
        private const val MODEL_BASE = "https://fc39e5fc.pinme.dev"
        const val MODELS_ROOT = "models"
        // 每个模型 id 一把静态锁: download 跑在多线程池上, 同 id 并发会在 deleteRecursively()
        // 与写文件之间互踩, 损坏模型目录且无自愈. 静态 = 跨实例生效 (主界面/悬浮窗各有
        // 一个 ModelBridge 实例, 各带自己的线程池).
        private val downloadLocks = java.util.concurrent.ConcurrentHashMap<String, Any>()

        // ===== 自定义模型导入（2026-11-03 新增）=====
        /** zip 本体上限 */
        private const val IMPORT_MAX_ZIP_BYTES = 200L * 1024 * 1024
        /** 解压后累计上限 */
        private const val IMPORT_MAX_TOTAL_BYTES = 600L * 1024 * 1024
        /** 解压后单文件上限 */
        private const val IMPORT_MAX_ENTRY_BYTES = 200L * 1024 * 1024
        /** 条目数上限 */
        private const val IMPORT_MAX_ENTRIES = 8000
    }

    private fun lockFor(id: String): Any = downloadLocks.computeIfAbsent(id) { Any() }

    val modelsDir: File

        get() = File(appContext.filesDir, MODELS_ROOT)


    @android.webkit.JavascriptInterface
    fun download(id: String) {
        ioExecutor.execute {
            val json = synchronized(lockFor(id)) {
                runCatching {
                    val modelDir = modelsDir.resolve(safeSegment(id))
                    val existing = findEntryBase(modelDir)
                    if (existing != null) return@runCatching ok(existing)
                    val url = getDownloadUrl(id)
                    ok(installZipStreaming(id, url))
                }.getOrElse { e ->
                    err(e.message ?: "下载失败")
                }
            }
            postToJs("__noriModelRes", json)
        }
    }

    private fun postToJs(fn: String, json: String) {
        mainHandler.post {
            runCatching {
                webView?.evaluateJavascript("window.$fn && window.$fn(${JSONObject.quote(json)})", null)
            }
        }
    }

    // ================= 自定义模型导入（2026-11-03 新增）=================

    /**
     * 从系统文件选择器选中的 ZIP 导入模型。
     * 前端调用链：window.NoriImport.pick() → 原生选择器 → 本方法 → window.__noriModelImportRes(json)
     * 返回 JSON：{ok:true,id,entryBase} 或 {ok:false,message}
     */
    @android.webkit.JavascriptInterface
    fun importZip(uriString: String) {
        ioExecutor.execute {
            val json = synchronized(lockFor("__import__")) {
                runCatching { installFromUri(android.net.Uri.parse(uriString)) }
                    .getOrElse { e -> err(e.message ?: "导入失败") }
            }
            postToJs("__noriModelImportRes", json)
        }
    }

    private fun installFromUri(uri: android.net.Uri): String {
        val resolver = appContext.contentResolver
        modelsDir.mkdirs()
        val tmp = File(modelsDir, ".import.zip.tmp")
        val staging = File(modelsDir, ".import.staging.tmp")
        try {
            // 1) 流式复制到临时文件，同时限制体积
            var written = 0L
            val input = resolver.openInputStream(uri) ?: throw RuntimeException("无法读取所选文件")
            input.use { ins ->
                FileOutputStream(tmp).use { out ->
                    val buf = ByteArray(64 * 1024)
                    while (true) {
                        val n = ins.read(buf)
                        if (n < 0) break
                        written += n
                        if (written > IMPORT_MAX_ZIP_BYTES) throw RuntimeException("压缩包超过 200MB 上限")
                        out.write(buf, 0, n)
                    }
                }
            }
            if (written == 0L) throw RuntimeException("所选文件为空")

            // 2) 第一遍：只扫条目名（不读内容），找最浅的 .model3.json，并拒绝非法路径
            var best: String? = null
            var entryCount = 0
            ZipInputStream(tmp.inputStream().buffered()).use { zip ->
                var e = zip.nextEntry
                while (e != null) {
                    if (!e.isDirectory) {
                        entryCount++
                        if (entryCount > IMPORT_MAX_ENTRIES) throw RuntimeException("压缩包条目过多")
                        val raw = e.name.replace('\\', '/')
                        if (raw.startsWith("/") || raw.split("/").any { it == ".." }) {
                            throw RuntimeException("压缩包包含非法路径")
                        }
                        val name = sanitize(raw)
                        if (name.endsWith(".model3.json", true)) {
                            val cur = best
                            if (cur == null || name.count { c -> c == '/' } < cur.count { c -> c == '/' }) best = name
                        }
                    }
                    zip.closeEntry()
                    e = zip.nextEntry
                }
            }
            val bestPath = best ?: throw RuntimeException("压缩包里没有 .model3.json")
            val parts = bestPath.split("/")
            val entryBase = parts[parts.size - 1].removeSuffix(".model3.json").trim()
            if (entryBase.isEmpty()) throw RuntimeException("模型入口名无效")
            val id = safeModelId(entryBase)
            val target = modelsDir.resolve(id)
            if (target.exists()) throw RuntimeException("已存在同名模型（" + entryBase + "），请先删除再导入")
            val prefix = parts.dropLast(1).joinToString("/")

            // 3) 第二遍：流式解压到暂存目录，累计体积、逐条校验路径
            if (staging.exists()) staging.deleteRecursively()
            staging.mkdirs()
            val outBase = File(staging, safeSegment(entryBase))
            var total = 0L
            ZipInputStream(tmp.inputStream().buffered()).use { zip ->
                var e = zip.nextEntry
                while (e != null) {
                    if (!e.isDirectory) {
                        var rel = sanitize(e.name)
                        if (prefix.isNotEmpty() && rel.startsWith(prefix + "/")) {
                            rel = rel.removePrefix(prefix + "/")
                        }
                        // 画师源文件（PSD/PSB/CLIP）运行时用不到，且往往上百 MB：
                        // 直接跳过，既不占空间也不会撑爆单文件上限
                        val lower = rel.lowercase()
                        if (lower.endsWith(".psd") || lower.endsWith(".psb") || lower.endsWith(".clip")) {
                            zip.closeEntry()
                            e = zip.nextEntry
                            continue
                        }
                        val out = outBase.resolve(sanitizeSegments(rel, entryBase))
                        out.parentFile?.mkdirs()
                        if (!isSafeChild(out, staging)) throw RuntimeException("压缩包包含非法路径")
                        var size = 0L
                        FileOutputStream(out).use { o ->
                            val buf = ByteArray(64 * 1024)
                            while (true) {
                                val n = zip.read(buf)
                                if (n < 0) break
                                size += n
                                total += n
                                if (size > IMPORT_MAX_ENTRY_BYTES) throw RuntimeException("压缩包内单个文件过大：" + rel)
                                if (total > IMPORT_MAX_TOTAL_BYTES) throw RuntimeException("解压后体积超过 600MB 上限")
                                o.write(buf, 0, n)
                            }
                        }
                    }
                    zip.closeEntry()
                    e = zip.nextEntry
                }
            }
            if (outBase.walkTopDown().none { it.isFile }) throw RuntimeException("解压后没有文件")

            // 4) 校验 .model3.json 及它引用的核心资源
            val entryJson = File(outBase, entryBase + ".model3.json")
            if (!entryJson.isFile) throw RuntimeException("找不到入口文件 " + entryBase + ".model3.json")
            validateModelJson(entryJson, outBase)

            // 5) 原子换名（换名失败则退化为复制）
            if (!staging.renameTo(target)) {
                staging.copyRecursively(target, overwrite = true)
                staging.deleteRecursively()
            }
            return JSONObject().put("ok", true).put("id", id).put("entryBase", entryBase).toString()
        } finally {
            tmp.delete()
            staging.deleteRecursively()
        }
    }

    /** 校验 model3.json 里引用的核心文件确实存在（Moc / 全部 Textures / Physics） */
    private fun validateModelJson(json: File, root: File) {
        val text = json.readText(Charsets.UTF_8)
        val obj = try {
            JSONObject(text)
        } catch (e: Exception) {
            throw RuntimeException("模型配置不是合法 JSON")
        }
        val refs = obj.optJSONObject("FileReferences") ?: throw RuntimeException("模型配置缺少 FileReferences")
        val moc = refs.optString("Moc", "")
        if (moc.isNotEmpty() && !File(root, moc).isFile) throw RuntimeException("缺少模型文件：" + moc)
        val textures = refs.optJSONArray("Textures")
        if (textures != null) {
            for (i in 0 until textures.length()) {
                val t = textures.optString(i, "")
                if (t.isNotEmpty() && !File(root, t).isFile) throw RuntimeException("缺少贴图：" + t)
            }
        }
        val physics = refs.optString("Physics", "")
        if (physics.isNotEmpty() && !File(root, physics).isFile) throw RuntimeException("缺少物理文件：" + physics)
    }

    // ================= 导入功能结束 =================

    @android.webkit.JavascriptInterface
    fun listInstalled(): String {
        val arr = JSONArray()
        runCatching {
            val root = modelsDir
            root.listFiles()?.forEach { dir ->
                dir.takeIf { it.isDirectory }?.let { d ->
                    // 跳过导入/下载的中间目录
                    if (d.name.startsWith(".")) return@let
                    findEntryBase(d)?.let { base ->
                        // Stat metadata also invalidates cached geometry after model replacement.
                        val fingerprint = d.walkTopDown().filter { it.isFile }
                            .sortedBy { it.relativeTo(d).path }
                            .joinToString("\n") { "${it.relativeTo(d).path}:${it.length()}:${it.lastModified()}" }
                        val revision = MessageDigest.getInstance("SHA-256")
                            .digest(fingerprint.toByteArray(Charsets.UTF_8)).joinToString("") { "%02x".format(it) }
                        arr.put(JSONObject().put("id", d.name).put("entryBase", base).put("revision", revision))
                    }
                }
            }
        }
        return arr.toString()
    }

    // 给前端"删除模型"用的别名（与原有 delete 同名，保持兼容）
    @android.webkit.JavascriptInterface
    fun deleteModel(id: String) {
        runCatching { modelsDir.resolve(safeSegment(id)).deleteRecursively() }
    }

    
    @android.webkit.JavascriptInterface
    fun delete(id: String) {
        runCatching { modelsDir.resolve(safeSegment(id)).deleteRecursively() }
    }

    
    private fun findEntryBase(modelDir: File): String? {
        if (!modelDir.isDirectory) return null
        return modelDir.walkTopDown()
            .filter { it.isFile && it.name.endsWith("model3.json", true) }
            .mapNotNull { f ->
                val rel = f.toRelativeString(modelDir).replace('\\', '/')
                val parts = rel.split("/").filter { it.isNotEmpty() }
                val base = when {
                    parts.isEmpty()-> null
                    parts.size <= 1 -> f.name.removeSuffix(".model3.json")
                    else -> parts[0]
                }
                base?.takeIf { it.isNotBlank() }
            }
            .minByOrNull { depthOfEntry(modelDir, it) }
    }

    
    private fun depthOfEntry(modelDir: File, entryBase: String): Int {
        val dir = modelDir.resolve(safeModelId(entryBase))
        val f = File(dir, "$entryBase.model3.json")
        return if (f.exists()) 0 else 1
    }

    /** 模型 zip 直链。原来是"先请求网关拿下发地址"再下载, 现在静态托管直接拼:
     *  少一次网络往返、少一个失败点 (网关挂了就完全下不了模型)。 */
    private fun getDownloadUrl(id: String): String = "$MODEL_BASE/${encode(id)}.zip"

    /** 流式下载到文件: 边下边写 (64KB 缓冲), 不再把整个 zip 读进内存 ——
     *  原实现 readBytes() 峰值内存 ≈ 包体大小, 几十 MB 的模型包在大内存占用时直接 OOM */
    private fun downloadToFile(url: String, out: File) {
        val u = URL(url)
        val conn = (u.openConnection() as HttpURLConnection).apply {
            connectTimeout = 15_000
            readTimeout = 30_000
        }
        try {
            if (conn.responseCode !in 200..299) throw RuntimeException("下载 ZIP 失败: HTTP ${conn.responseCode}")
            val outPath = out.canonicalFile.toPath()
            conn.inputStream.use { input ->
                java.nio.file.Files.newOutputStream(outPath).use { fos ->
                    input.copyTo(fos, 64 * 1024)
                    fos.flush()
                }
            }
        } finally {
            conn.disconnect()
        }
    }

    /**
     * 流式安装: zip 落到临时文件后分两遍处理 (只扫条目名 / 逐条流式解压),
     * 任何条目都不整包进内存; 解压先进暂存目录, 成功后再换名 —— 解压中途失败
     * 不会像旧实现那样留下"删了旧的、新的只有一半"的损坏目录.
     */
    private fun installZipStreaming(id: String, url: String): String {
        val target = modelsDir.resolve(safeSegment(id))
        val tmp = File(modelsDir, "${safeSegment(id)}.download.tmp")
        val staging = File(modelsDir, "${safeSegment(id)}.install.tmp")
        try {
            modelsDir.mkdirs()
            downloadToFile(url, tmp)
            // 第一遍: 只读条目名, 选最浅的 .model3.json 作入口 (与旧实现同一选择规则)
            var best: String? = null
            ZipInputStream(tmp.inputStream().buffered()).use { zip ->
                var e = zip.nextEntry
                while (e != null) {
                    if (!e.isDirectory) {
                        val name = sanitize(e.name)
                        if (name.endsWith(".model3.json", true)) {
                            val cur = best
                            if (cur == null || name.count { c -> c == '/' } < cur.count { c -> c == '/' }) best = name
                        }
                    }
                    zip.closeEntry()
                    e = zip.nextEntry
                }
            }
            val bestPath = best ?: throw RuntimeException("模型包缺少 .model3.json")
            val parts = bestPath.split("/")
            val entryBase = parts[parts.size - 1].removeSuffix(".model3.json").trim()
            if (entryBase.isEmpty()) throw RuntimeException("模型入口名无效")
            val prefix = parts.dropLast(1).joinToString("/")
            // 第二遍: 流式解压到暂存目录 (逐条边读边写, 不缓存条目内容)
            if (staging.exists()) staging.deleteRecursively()
            staging.mkdirs()
            val outBase = File(staging, safeSegment(entryBase))
            ZipInputStream(tmp.inputStream().buffered()).use { zip ->
                var e = zip.nextEntry
                while (e != null) {
                    if (!e.isDirectory) {
                        var rel = sanitize(e.name)
                        if (prefix.isNotEmpty() && rel.startsWith("$prefix/")) rel = rel.removePrefix("$prefix/")
                        val out = outBase.resolve(sanitizeSegments(rel, entryBase))
                        out.parentFile?.mkdirs()
                        if (isSafeChild(out, staging)) {
                            FileOutputStream(out).use { o -> zip.copyTo(o) }
                        }
                    }
                    zip.closeEntry()
                    e = zip.nextEntry
                }
            }
            if (staging.walkTopDown().none { it.isFile }) throw RuntimeException("模型包为空或解压失败")
            // 原子换名: 校验通过才动现有目录
            if (target.exists()) target.deleteRecursively()
            if (!staging.renameTo(target)) {
                staging.copyRecursively(target, overwrite = true)
                staging.deleteRecursively()
            }
            return entryBase
        } finally {
            tmp.delete()
            staging.deleteRecursively()
        }
    }

    private fun isSafeChild(f: File, root: File): Boolean {
        return try {
            f.canonicalFile.toPath().startsWith(root.canonicalFile.toPath())
        } catch (_: Exception) {
            false
        }
    }

    private fun sanitizeSegments(rel: String, entryBase: String): String {
        val segs = rel.split("/").filter { it.isNotEmpty() && it != "." && it != ".." }
        return sanitize(segs.joinToString("/"))
    }

    private fun sanitize(raw: String): String = raw.replace("\\", "/")

    private fun safeSegment(s: String): String = s.replace(Regex("[^A-Za-z0-9._-]"), "_")

    /**
     * 模型目录名：保留中文等可读字符，只清掉路径分隔符与文件系统非法字符。
     * （原 safeSegment 会把「大肥鱼」变成 "___"，既不可读也会让前端列表显示成乱码。）
     */
    private fun safeModelId(s: String): String {
        val cleaned = s.replace(Regex("[\\\\/:*?\"<>|\\u0000-\\u001F]"), "_").trim()
        return if (cleaned.isEmpty() || cleaned == "." || cleaned == "..") "model" else cleaned
    }

    private fun encode(s: String): String = java.net.URLEncoder.encode(s, "UTF-8")

    private fun ok(entryBase: String): String =
        JSONObject().put("ok", true).put("entryBase", entryBase).toString()

    private fun err(msg: String): String =
        JSONObject().put("ok", false).put("message", msg).toString()
}
