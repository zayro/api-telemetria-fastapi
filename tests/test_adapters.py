import tempfile
import unittest
from pathlib import Path

from domain.errors import DocumentExtractionError, UnsupportedFileTypeError
from domain.models import ProtocolDocument

from infrastructure.adapters import (
    CorpusProtocolRetriever,
    GeminiEmbeddingAdapter,
    MultimodalDocumentExtractor,
)


class InMemoryIndexRepository:
    def __init__(self):
        self.documents = {}

    async def load_all(self):
        return list(self.documents.values())

    async def save_documents(self, documents):
        for document in documents:
            self.documents[document.id] = document

    async def replace_documents(self, source_file, documents):
        self.documents = {
            document_id: document
            for document_id, document in self.documents.items()
            if document.source_file != source_file
        }
        for document in documents:
            self.documents[document.id] = document


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_retriever_initializes_inside_event_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "protocolo_prueba.md").write_text(
                "# Protocolo de prueba\n\nContenido sobre cifrado TLS y puertos.",
                encoding="utf-8",
            )
            retriever = CorpusProtocolRetriever(
                protocols_dir=directory,
                embedding_service=GeminiEmbeddingAdapter(api_key=""),
            )

            await retriever.initialize()
            documents = await retriever.list_documents()

        self.assertGreater(len(documents), 0)
        file_document = next(
            document
            for document in documents
            if document.source_file == "protocolo_prueba.md"
        )
        self.assertEqual(file_document.doc_type, "markdown")
        self.assertIsNotNone(file_document.embedding)
        self.assertEqual(len(file_document.embedding), 128)

    async def test_search_uses_indexed_corpus(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "seguridad.md").write_text(
                "# Cifrado de datos\n\nEl cifrado AES-256 protege los datos en reposo.",
                encoding="utf-8",
            )
            retriever = CorpusProtocolRetriever(
                protocols_dir=directory,
                embedding_service=GeminiEmbeddingAdapter(api_key=""),
            )

            results = await retriever.search("cifrado AES-256", limit=3)

        self.assertGreater(len(results), 0)
        self.assertEqual(results[0].document.source_file, "seguridad.md")
        self.assertGreater(results[0].score, 0)

    async def test_persisted_documents_are_loaded_on_initialize(self):
        repository = InMemoryIndexRepository()
        embedding = GeminiEmbeddingAdapter(api_key="")

        first = CorpusProtocolRetriever(
            embedding_service=embedding, index_repository=repository
        )
        document = ProtocolDocument(
            id="file-subido-1",
            title="Documento subido",
            content="Contenido persistido sobre llaves criptográficas.",
            source_file="subido.pdf",
            category="seguridad",
            doc_type="pdf",
            metadata={"origin": "upload", "content_hash": "abc"},
        )
        await first.index_documents([document], replace_source="subido.pdf")

        second = CorpusProtocolRetriever(
            embedding_service=embedding, index_repository=repository
        )
        await second.initialize()
        loaded = await second.get_document("file-subido-1")

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.source_file, "subido.pdf")
        self.assertIsNotNone(loaded.embedding)

    async def test_pseudo_embedding_is_deterministic_between_instances(self):
        first = GeminiEmbeddingAdapter(api_key="")._pseudo_embedding(
            "protocolo de seguridad tls con certificados"
        )
        second = GeminiEmbeddingAdapter(api_key="")._pseudo_embedding(
            "protocolo de seguridad tls con certificados"
        )
        self.assertEqual(first, second)

    def test_embedding_defaults_use_current_gemini_model(self):
        adapter = GeminiEmbeddingAdapter(api_key="test-key")
        self.assertEqual(adapter.model_name, "gemini-embedding-001")
        self.assertEqual(adapter.dimension, 768)

    async def test_extractor_indexes_real_pdf_text(self):
        pdf_path = (
            Path(__file__).resolve().parents[1]
            / "data"
            / "protocols"
            / "protocolo_cifrado_datos.pdf"
        )
        extractor = MultimodalDocumentExtractor()
        title, text, doc_type, metadata = await extractor.extract_content(
            pdf_path.name, pdf_path.read_bytes()
        )

        self.assertEqual(doc_type, "pdf")
        self.assertIn("Protocolo", title)
        self.assertIn("AES-256", text)
        self.assertGreaterEqual(metadata["pages"], 1)

    async def test_extractor_rejects_unsupported_and_invalid_files(self):
        extractor = MultimodalDocumentExtractor()
        with self.assertRaises(UnsupportedFileTypeError):
            await extractor.extract_content("programa.exe", b"MZ-fake")

        with self.assertRaises(UnsupportedFileTypeError):
            await extractor.extract_content("falso.pdf", b"no es un pdf")

        with self.assertRaises(DocumentExtractionError):
            await extractor.extract_content("diagrama.png", b"\x89PNG\r\n\x1a\nresto")


if __name__ == "__main__":
    unittest.main()
