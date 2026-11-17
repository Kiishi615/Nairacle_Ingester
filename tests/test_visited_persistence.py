import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('dotenv.load_dotenv', lambda *a, **kw: False)
    for key in ['OPENAI_API_KEY', 'PINECONE_API_KEY', 'PINECONE_INDEX_NAME']:
        monkeypatch.setenv(key, 'test-only')
    old_handlers = logging.getLogger().handlers[:]
    module = load_module('pipeline_under_test', ROOT / 'daily_ingest.py')
    for key, name in [('VISITED_FILE', 'visited_urls.txt'), ('MASTER_FILE', 'master.jsonl'),
                      ('SCRAPED_FILE', 'scraped.jsonl'), ('CLASSIFIED_FILE', 'classified.jsonl'),
                      ('RETRY_FILE', 'retry_urls.txt')]:
        setattr(module, key, tmp_path / name)
    module.VISITED_FILE.write_text('https://example.com/old/\n')
    module.EMBED_BATCH_SIZE = 1
    yield module
    for handler in logging.getLogger().handlers:
        handler.close()
    logging.getLogger().handlers = old_handlers


def record(name):
    return dict(url=f'https://example.com/{name}/', title=name, date='2026-11-17',
                categories=[], summary='A sufficiently informative summary.', content='x' * 100)


def clients(monkeypatch, *, existing=(), upsert=None, embeddings=None):
    index = NS(fetch=Mock(return_value=NS(vectors={x: {} for x in existing})),
               upsert=upsert or Mock(side_effect=lambda **kw: NS(upserted_count=len(kw['vectors']))),
               describe_index_stats=Mock(return_value=NS(total_vector_count=1)))
    embed = embeddings or Mock(side_effect=lambda **kw: NS(data=[NS(index=i, embedding=[0.1]) for i in range(len(kw['input']))]))
    monkeypatch.setitem(sys.modules, 'openai', NS(OpenAI=lambda **kw: NS(embeddings=NS(create=embed))))
    monkeypatch.setitem(sys.modules, 'pinecone', NS(Pinecone=lambda **kw: NS(Index=lambda **kw: index)))
    return index, embed


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def test_scraping_and_empty_confirmation_never_change_history(pipeline):
    before = pipeline.VISITED_FILE.read_bytes()
    pipeline.mark_visited('https://example.com/pending/')
    pipeline.persist_visited_urls([])
    assert pipeline.VISITED_FILE.read_bytes() == before


def test_partial_upload_only_checkpoints_success_and_keeps_failures(pipeline, monkeypatch):
    a, b, discarded = record('ok'), record('failed'), record('discarded')
    pipeline.save_scraped([a, b, discarded])
    clients(monkeypatch, upsert=Mock(side_effect=[NS(upserted_count=1), RuntimeError('upload failed')]))
    pipeline.run_upsert([a, b])
    assert set(pipeline.VISITED_FILE.read_text().splitlines()) == {'https://example.com/old/', a['url']}
    assert {r['url'] for r in rows(pipeline.MASTER_FILE)} == {a['url'].rstrip('/')}
    assert {r['url'] for r in rows(pipeline.SCRAPED_FILE)} == {b['url'], discarded['url']}
    assert a['url'].endswith('/')  # Database normalization must not mutate caller records.
    assert pipeline.report['upsert_errors'] == 1


@pytest.mark.parametrize('failure', ['embedding', 'missing_embedding', 'short_ack', 'upsert'])
def test_failure_does_not_mark_or_remove_article(pipeline, monkeypatch, failure):
    a = record('pending')
    pipeline.save_scraped([a])
    before = pipeline.VISITED_FILE.read_bytes()
    kwargs = {}
    if failure == 'embedding': kwargs['embeddings'] = Mock(side_effect=RuntimeError('embedding failed'))
    if failure == 'missing_embedding': kwargs['embeddings'] = Mock(return_value=NS(data=[]))
    if failure == 'short_ack': kwargs['upsert'] = Mock(return_value=NS(upserted_count=0))
    if failure == 'upsert': kwargs['upsert'] = Mock(side_effect=RuntimeError('upload failed'))
    index, _ = clients(monkeypatch, **kwargs)
    pipeline.run_upsert([a])
    assert pipeline.VISITED_FILE.read_bytes() == before
    assert rows(pipeline.SCRAPED_FILE) == [a]
    assert rows(pipeline.MASTER_FILE) == []
    if failure in ('embedding', 'missing_embedding'): index.upsert.assert_not_called()


def test_verified_existing_record_is_checkpointed_without_upload(pipeline, monkeypatch):
    a = record('already-ingested')
    pipeline.save_scraped([a])
    index, embed = clients(monkeypatch, existing=[a['url'].rstrip('/')])
    pipeline.run_upsert([a])
    assert a['url'] in pipeline.VISITED_FILE.read_text().splitlines()
    assert rows(pipeline.SCRAPED_FILE) == []
    index.upsert.assert_not_called()
    embed.assert_not_called()


def test_confirmed_batch_survives_later_interruption(pipeline, monkeypatch):
    a, b = record('done'), record('interrupted')
    pipeline.save_scraped([a, b])
    clients(monkeypatch, upsert=Mock(side_effect=[NS(upserted_count=1), KeyboardInterrupt()]))
    with pytest.raises(KeyboardInterrupt): pipeline.run_upsert([a, b])
    assert set(pipeline.VISITED_FILE.read_text().splitlines()) == {'https://example.com/old/', a['url']}


def test_atomic_write_failure_preserves_old_history(pipeline, monkeypatch):
    before = pipeline.VISITED_FILE.read_bytes()
    monkeypatch.setattr(pipeline.os, 'replace', Mock(side_effect=OSError('disk failure')))
    with pytest.raises(OSError): pipeline.persist_visited_urls(['https://example.com/confirmed/'])
    assert pipeline.VISITED_FILE.read_bytes() == before


def test_missing_baseline_is_not_recreated(pipeline):
    pipeline.VISITED_FILE.unlink()
    with pytest.raises(FileNotFoundError): pipeline.persist_visited_urls(['https://example.com/confirmed/'])
    assert not pipeline.VISITED_FILE.exists()


def test_classify_only_keeps_existing_history(pipeline, monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['daily_ingest.py', '--classify-only'])
    for name in ['run_backfill', 'report_backfill_failures', 'run_dedup', 'run_clean', 'write_report']:
        monkeypatch.setattr(pipeline, name, Mock())
    a = record('confirmed')
    monkeypatch.setattr(pipeline, 'run_classify', Mock(return_value=[a]))
    monkeypatch.setattr(pipeline, 'run_upsert', lambda kept: pipeline.persist_visited_urls(r['url'] for r in kept))
    pipeline.seen.clear()
    pipeline.main()
    assert set(pipeline.VISITED_FILE.read_text().splitlines()) == {'https://example.com/old/', a['url']}


def test_retry_urls_restore_without_losing_local_article_records(pipeline):
    a = record('pending')
    pipeline.save_scraped([a])
    pipeline.RETRY_FILE.write_text(a['url'] + '\nhttps://example.com/need-scrape/\nhttps://example.com/old/\n')
    pipeline.restore_pending_articles()
    assert pipeline.load_scraped() == [a, pipeline.pending_article('https://example.com/need-scrape/')]
    pipeline.report_backfill_failures()
    assert pipeline.report['backfill_failures'] == 1
    assert len(pipeline.load_scraped()) == 2


def test_model_failure_is_distinct_from_discard(pipeline):
    a = record('article')
    assert pipeline._classify_one(Mock(invoke=Mock(side_effect=RuntimeError('bad response'))), a) == ('retry', None)
    assert pipeline._classify_one(Mock(invoke=Mock(return_value=NS(decision='keep', summary=''))), a) == ('retry', None)
    assert pipeline._classify_one(Mock(invoke=Mock(return_value=NS(decision='discard', summary=None))), a) == ('discard', None)


def test_publish_preserves_concurrent_commits_and_updates_retry_queue(tmp_path, monkeypatch):
    publisher = load_module('publisher_under_test', ROOT / 'scripts/publish_visited_urls.py')
    real_run = subprocess.run
    def git(cwd, *args):
        result = real_run(['git', *args], cwd=cwd, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()
    origin, seed, ours, other = [tmp_path / n for n in ['origin.git', 'seed', 'ours', 'other']]
    git(tmp_path, 'init', '--bare', '--initial-branch=main', str(origin))
    git(tmp_path, 'clone', origin.as_posix(), seed.as_posix())
    for path in [seed]:
        git(path, 'config', 'user.name', 'Test'); git(path, 'config', 'user.email', 'test@example.com')
    (seed / 'visited_urls.txt').write_text('https://example.com/old/\n')
    (seed / 'retry_urls.txt').write_text('https://example.com/resolved/\n')
    (seed / 'README.md').write_text('initial\n')
    git(seed, 'add', '.'); git(seed, 'commit', '-m', 'initial'); git(seed, 'push', 'origin', 'main')
    git(tmp_path, 'clone', origin.as_posix(), ours.as_posix())
    git(tmp_path, 'clone', origin.as_posix(), other.as_posix())
    git(other, 'config', 'user.name', 'Test'); git(other, 'config', 'user.email', 'test@example.com')
    (ours / 'visited_urls.txt').write_text('https://example.com/old/\nhttps://example.com/ours/\n')
    (ours / 'scraped_today.jsonl').write_text(json.dumps(record('pending')) + '\n')
    (ours / 'do-not-publish.txt').write_text('unrelated local file')
    (other / 'README.md').write_text('concurrent edit\n')
    (other / 'visited_urls.txt').write_text('https://example.com/old/\nhttps://example.com/theirs/\n')
    (other / 'retry_urls.txt').write_text('https://example.com/resolved/\nhttps://example.com/their-retry/\n')
    git(other, 'add', '.'); git(other, 'commit', '-m', 'concurrent change'); git(other, 'push', 'origin', 'main')
    interrupted = False
    def racing_run(command, **kwargs):
        nonlocal interrupted
        if command[:3] == ['git', 'push', 'origin'] and not interrupted:
            interrupted = True
            with (other / 'visited_urls.txt').open('a') as f: f.write('https://example.com/racing/\n')
            git(other, 'add', 'visited_urls.txt'); git(other, 'commit', '-m', 'racing update'); git(other, 'push', 'origin', 'main')
        return real_run(command, **kwargs)
    monkeypatch.setattr(publisher.subprocess, 'run', racing_run)
    publisher.publish(ours, 'main')
    got = set(git(origin, 'show', 'main:visited_urls.txt').splitlines())
    assert got == {f'https://example.com/{x}/' for x in ['old', 'ours', 'theirs', 'racing']}
    assert git(origin, 'show', 'main:README.md') == 'concurrent edit'
    assert set(git(origin, 'diff-tree', '--no-commit-id', '--name-only', '-r', 'main').splitlines()) == {'visited_urls.txt', 'retry_urls.txt'}
    assert set(git(origin, 'show', 'main:retry_urls.txt').splitlines()) == {
        'https://example.com/pending/', 'https://example.com/their-retry/',
    }
    assert 'do-not-publish.txt' not in git(origin, 'ls-tree', '--name-only', 'main')
    before = git(origin, 'rev-parse', 'main')
    publisher.publish(ours, 'main')
    assert git(origin, 'rev-parse', 'main') == before
