"""
Comparación Exhaustiva de Embeddings para Seq2Seq Summarization

Este script compara:
1. Embeddings No-Contextuales (FastText):
   - Frozen (congelado)
   - Fine-tuned (ajustado durante entrenamiento)
   - From scratch (inicializado aleatoriamente)

2. Embeddings Contextuales (BERT):
   - Frozen (congelado)
   - Fine-tuned (ajustado durante entrenamiento)

Cumple con los requisitos de comparar diferentes tipos de embeddings
y estrategias de fine-tuning.
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import pandas as pd
import numpy as np
from gensim.models import KeyedVectors
from transformers import BertModel, BertTokenizer
from sklearn.model_selection import train_test_split
import random
from tqdm import tqdm
import os
import json
from datetime import datetime

# Set random seeds
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Hyperparameters
MAX_HEADLINE_LEN = 20
MAX_ARTICLE_LEN = 512
HIDDEN_SIZE = 256
EMBEDDING_DIM = 300  # For FastText/Word2Vec
BERT_DIM = 768
BATCH_SIZE = 32
LEARNING_RATE = 0.001
NUM_EPOCHS = 15
TEACHER_FORCING_RATIO = 0.5
PATIENCE = 3

# Special tokens
PAD_TOKEN = '<PAD>'
SOS_TOKEN = '<SOS>'
EOS_TOKEN = '<EOS>'
UNK_TOKEN = '<UNK>'


class Vocabulary:
    """Build vocabulary from text"""
    def __init__(self):
        self.word2idx = {PAD_TOKEN: 0, SOS_TOKEN: 1, EOS_TOKEN: 2, UNK_TOKEN: 3}
        self.idx2word = {0: PAD_TOKEN, 1: SOS_TOKEN, 2: EOS_TOKEN, 3: UNK_TOKEN}
        self.word_count = {}
        self.n_words = 4
    
    def add_sentence(self, sentence):
        for word in sentence.split():
            self.add_word(word)
    
    def add_word(self, word):
        if word not in self.word2idx:
            self.word2idx[word] = self.n_words
            self.idx2word[self.n_words] = word
            self.word_count[word] = 1
            self.n_words += 1
        else:
            self.word_count[word] += 1


class SummarizationDataset(Dataset):
    """Dataset for summarization task"""
    def __init__(self, articles, headlines, vocab, max_article_len, max_headline_len):
        self.articles = articles
        self.headlines = headlines
        self.vocab = vocab
        self.max_article_len = max_article_len
        self.max_headline_len = max_headline_len
    
    def __len__(self):
        return len(self.articles)
    
    def __getitem__(self, idx):
        article = self.articles[idx]
        headline = self.headlines[idx]
        
        article_indices = [self.vocab.word2idx.get(word, self.vocab.word2idx[UNK_TOKEN]) 
                          for word in article.split()[:self.max_article_len]]
        headline_indices = [self.vocab.word2idx[SOS_TOKEN]] + \
                          [self.vocab.word2idx.get(word, self.vocab.word2idx[UNK_TOKEN]) 
                           for word in headline.split()[:self.max_headline_len-1]] + \
                          [self.vocab.word2idx[EOS_TOKEN]]
        
        article_len = len(article_indices)
        headline_len = len(headline_indices)
        
        article_indices += [self.vocab.word2idx[PAD_TOKEN]] * (self.max_article_len - article_len)
        headline_indices += [self.vocab.word2idx[PAD_TOKEN]] * (self.max_headline_len + 1 - headline_len)
        
        return {
            'article': torch.tensor(article_indices, dtype=torch.long),
            'headline': torch.tensor(headline_indices, dtype=torch.long),
            'article_len': article_len,
            'headline_len': headline_len
        }


class EncoderRNN(nn.Module):
    """Bidirectional GRU Encoder with configurable embeddings"""
    def __init__(self, vocab_size, embedding_dim, hidden_size, embedding_weights=None, 
                 freeze_embeddings=False, use_pretrained=True):
        super(EncoderRNN, self).__init__()
        self.hidden_size = hidden_size
        self.freeze_embeddings = freeze_embeddings
        
        if use_pretrained and embedding_weights is not None:
            self.embedding = nn.Embedding.from_pretrained(embedding_weights, 
                                                         freeze=freeze_embeddings, 
                                                         padding_idx=0)
        else:
            # From scratch
            self.embedding = nn.Embedding(vocab_size, embedding_dim, padding_idx=0)
        
        self.gru = nn.GRU(embedding_dim, hidden_size, batch_first=True, bidirectional=True)
    
    def forward(self, input_seq, input_lengths):
        embedded = self.embedding(input_seq)
        
        packed = nn.utils.rnn.pack_padded_sequence(embedded, input_lengths.cpu(), 
                                                    batch_first=True, enforce_sorted=False)
        outputs, hidden = self.gru(packed)
        outputs, _ = nn.utils.rnn.pad_packed_sequence(outputs, batch_first=True)
        
        outputs = outputs[:, :, :self.hidden_size] + outputs[:, :, self.hidden_size:]
        hidden = hidden[0] + hidden[1]
        hidden = hidden.unsqueeze(0)
        
        return outputs, hidden


class BahdanauAttention(nn.Module):
    """Bahdanau Attention Mechanism"""
    def __init__(self, hidden_size):
        super(BahdanauAttention, self).__init__()
        self.Wa = nn.Linear(hidden_size, hidden_size)
        self.Ua = nn.Linear(hidden_size, hidden_size)
        self.Va = nn.Linear(hidden_size, 1)
    
    def forward(self, query, keys):
        scores = self.Va(torch.tanh(self.Wa(query.unsqueeze(1)) + self.Ua(keys)))
        scores = scores.squeeze(2)
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), keys)
        context = context.squeeze(1)
        return context, weights


class AttnDecoderRNN(nn.Module):
    """GRU Decoder with Attention"""
    def __init__(self, vocab_size, embedding_dim, hidden_size, embedding_weights=None, 
                 freeze_embeddings=False, use_pretrained=True):
        super(AttnDecoderRNN, self).__init__()
        self.hidden_size = hidden_size
        self.vocab_size = vocab_size
        
        if use_pretrained and embedding_weights is not None:
            self.embedding = nn.Embedding.from_pretrained(embedding_weights, 
                                                         freeze=freeze_embeddings, 
                                                         padding_idx=0)
        else:
            self.embedding = nn.Embedding(vocab_size, embedding_dim, padding_idx=0)
        
        self.attention = BahdanauAttention(hidden_size)
        self.gru = nn.GRU(embedding_dim + hidden_size, hidden_size, batch_first=True)
        self.out = nn.Linear(hidden_size, vocab_size)
    
    def forward(self, input_token, hidden, encoder_outputs):
        embedded = self.embedding(input_token).unsqueeze(1)
        context, attn_weights = self.attention(hidden.squeeze(0), encoder_outputs)
        rnn_input = torch.cat([embedded, context.unsqueeze(1)], dim=2)
        output, hidden = self.gru(rnn_input, hidden)
        output = self.out(output.squeeze(1))
        return output, hidden, attn_weights


class Seq2Seq(nn.Module):
    """Seq2Seq Model Wrapper"""
    def __init__(self, encoder, decoder):
        super(Seq2Seq, self).__init__()
        self.encoder = encoder
        self.decoder = decoder
    
    def forward(self, src, src_len, trg, teacher_forcing_ratio=0.5):
        batch_size = src.size(0)
        trg_len = trg.size(1)
        trg_vocab_size = self.decoder.vocab_size
        
        outputs = torch.zeros(batch_size, trg_len, trg_vocab_size).to(src.device)
        encoder_outputs, hidden = self.encoder(src, src_len)
        input_token = trg[:, 0]
        
        for t in range(1, trg_len):
            output, hidden, _ = self.decoder(input_token, hidden, encoder_outputs)
            outputs[:, t] = output
            teacher_force = random.random() < teacher_forcing_ratio
            top1 = output.argmax(1)
            input_token = trg[:, t] if teacher_force else top1
        
        return outputs


class BERTEncoder(nn.Module):
    """Encoder using BERT + Bi-GRU"""
    def __init__(self, hidden_size, freeze_bert=True):
        super(BERTEncoder, self).__init__()
        self.bert = BertModel.from_pretrained('bert-base-uncased')
        self.freeze_bert = freeze_bert
        
        if freeze_bert:
            for param in self.bert.parameters():
                param.requires_grad = False
        
        self.gru = nn.GRU(BERT_DIM, hidden_size, batch_first=True, bidirectional=True)
        self.hidden_size = hidden_size
    
    def forward(self, input_ids, attention_mask):
        if self.freeze_bert:
            with torch.no_grad():
                bert_output = self.bert(input_ids=input_ids, attention_mask=attention_mask)
                embeddings = bert_output.last_hidden_state
        else:
            bert_output = self.bert(input_ids=input_ids, attention_mask=attention_mask)
            embeddings = bert_output.last_hidden_state
        
        outputs, hidden = self.gru(embeddings)
        outputs = outputs[:, :, :self.hidden_size] + outputs[:, :, self.hidden_size:]
        hidden = hidden[0] + hidden[1]
        hidden = hidden.unsqueeze(0)
        
        return outputs, hidden


class BERTAttnDecoder(nn.Module):
    """Decoder with BERT embeddings"""
    def __init__(self, vocab_size, hidden_size, freeze_bert=True):
        super(BERTAttnDecoder, self).__init__()
        self.bert = BertModel.from_pretrained('bert-base-uncased')
        self.freeze_bert = freeze_bert
        
        if freeze_bert:
            for param in self.bert.parameters():
                param.requires_grad = False
        
        self.attention = BahdanauAttention(hidden_size)
        self.gru = nn.GRU(BERT_DIM + hidden_size, hidden_size, batch_first=True)
        self.out = nn.Linear(hidden_size, vocab_size)
        self.vocab_size = vocab_size
    
    def forward(self, input_ids, hidden, encoder_outputs):
        if self.freeze_bert:
            with torch.no_grad():
                bert_output = self.bert(input_ids=input_ids.unsqueeze(1))
                embedded = bert_output.last_hidden_state
        else:
            bert_output = self.bert(input_ids=input_ids.unsqueeze(1))
            embedded = bert_output.last_hidden_state
        
        context, attn_weights = self.attention(hidden.squeeze(0), encoder_outputs)
        rnn_input = torch.cat([embedded, context.unsqueeze(1)], dim=2)
        output, hidden = self.gru(rnn_input, hidden)
        output = self.out(output.squeeze(1))
        
        return output, hidden, attn_weights


class BERTSummarizationDataset(Dataset):
    """Dataset using BERT tokenizer"""
    def __init__(self, articles, headlines, tokenizer, max_article_len, max_headline_len):
        self.articles = articles
        self.headlines = headlines
        self.tokenizer = tokenizer
        self.max_article_len = max_article_len
        self.max_headline_len = max_headline_len
    
    def __len__(self):
        return len(self.articles)
    
    def __getitem__(self, idx):
        article = self.articles[idx]
        headline = self.headlines[idx]
        
        article_encoding = self.tokenizer(
            article, max_length=self.max_article_len,
            padding='max_length', truncation=True, return_tensors='pt'
        )
        
        headline_encoding = self.tokenizer(
            headline, max_length=self.max_headline_len,
            padding='max_length', truncation=True, return_tensors='pt'
        )
        
        return {
            'article_ids': article_encoding['input_ids'].squeeze(0),
            'article_mask': article_encoding['attention_mask'].squeeze(0),
            'headline_ids': headline_encoding['input_ids'].squeeze(0),
            'headline_mask': headline_encoding['attention_mask'].squeeze(0)
        }


class BERTSeq2Seq(nn.Module):
    """Seq2Seq with BERT embeddings"""
    def __init__(self, encoder, decoder, tokenizer):
        super(BERTSeq2Seq, self).__init__()
        self.encoder = encoder
        self.decoder = decoder
        self.tokenizer = tokenizer
    
    def forward(self, src_ids, src_mask, trg_ids, teacher_forcing_ratio=0.5):
        batch_size = src_ids.size(0)
        trg_len = trg_ids.size(1)
        vocab_size = self.decoder.vocab_size
        
        outputs = torch.zeros(batch_size, trg_len, vocab_size).to(src_ids.device)
        encoder_outputs, hidden = self.encoder(src_ids, src_mask)
        input_token = trg_ids[:, 0]
        
        for t in range(1, trg_len):
            output, hidden, _ = self.decoder(input_token, hidden, encoder_outputs)
            outputs[:, t] = output
            teacher_force = random.random() < teacher_forcing_ratio
            top1 = output.argmax(1)
            input_token = trg_ids[:, t] if teacher_force else top1
        
        return outputs


def load_fasttext_embeddings(vocab, model_path):
    """Load FastText embeddings"""
    print(f"Loading FastText model from {model_path}...")
    model = KeyedVectors.load(model_path)
    
    embedding_matrix = np.zeros((vocab.n_words, EMBEDDING_DIM))
    found = 0
    
    for word, idx in vocab.word2idx.items():
        if word in model:
            embedding_matrix[idx] = model[word]
            found += 1
        else:
            embedding_matrix[idx] = np.random.normal(0, 0.1, EMBEDDING_DIM)
    
    print(f"Found {found}/{vocab.n_words} words in FastText model")
    return torch.FloatTensor(embedding_matrix)


def train_epoch(model, dataloader, optimizer, criterion, teacher_forcing_ratio, is_bert=False):
    """Train for one epoch"""
    model.train()
    epoch_loss = 0
    
    for batch in tqdm(dataloader, desc="Training"):
        if is_bert:
            article_ids = batch['article_ids'].to(device)
            article_mask = batch['article_mask'].to(device)
            headline_ids = batch['headline_ids'].to(device)
            
            optimizer.zero_grad()
            output = model(article_ids, article_mask, headline_ids, teacher_forcing_ratio)
            
            output_dim = output.shape[-1]
            output = output[:, 1:].reshape(-1, output_dim)
            headline = headline_ids[:, 1:].reshape(-1)
        else:
            article = batch['article'].to(device)
            headline = batch['headline'].to(device)
            article_len = batch['article_len']
            
            optimizer.zero_grad()
            output = model(article, article_len, headline, teacher_forcing_ratio)
            
            output_dim = output.shape[-1]
            output = output[:, 1:].reshape(-1, output_dim)
            headline = headline[:, 1:].reshape(-1)
        
        loss = criterion(output, headline)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def evaluate(model, dataloader, criterion, is_bert=False):
    """Evaluate the model"""
    model.eval()
    epoch_loss = 0
    
    with torch.no_grad():
        for batch in dataloader:
            if is_bert:
                article_ids = batch['article_ids'].to(device)
                article_mask = batch['article_mask'].to(device)
                headline_ids = batch['headline_ids'].to(device)
                
                output = model(article_ids, article_mask, headline_ids, 0)
                
                output_dim = output.shape[-1]
                output = output[:, 1:].reshape(-1, output_dim)
                headline = headline_ids[:, 1:].reshape(-1)
            else:
                article = batch['article'].to(device)
                headline = batch['headline'].to(device)
                article_len = batch['article_len']
                
                output = model(article, article_len, headline, 0)
                
                output_dim = output.shape[-1]
                output = output[:, 1:].reshape(-1, output_dim)
                headline = headline[:, 1:].reshape(-1)
            
            loss = criterion(output, headline)
            epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def calculate_rouge_n(reference, hypothesis, n=1):
    """Simple n-gram overlap calculation"""
    def get_ngrams(text, n):
        words = text.split()
        return set([' '.join(words[i:i+n]) for i in range(len(words)-n+1)])
    
    ref_ngrams = get_ngrams(reference, n)
    hyp_ngrams = get_ngrams(hypothesis, n)
    
    if len(ref_ngrams) == 0:
        return 0.0
    
    overlap = len(ref_ngrams & hyp_ngrams)
    return overlap / len(ref_ngrams)


def run_experiment(config_name, model, train_loader, val_loader, test_loader, 
                   criterion, optimizer, is_bert=False):
    """Run a single experiment configuration"""
    print(f"\n{'='*80}")
    print(f"EXPERIMENT: {config_name}")
    print(f"{'='*80}")
    
    best_val_loss = float('inf')
    patience_counter = 0
    results = {
        'config': config_name,
        'train_losses': [],
        'val_losses': [],
        'best_epoch': 0,
        'best_val_loss': float('inf'),
        'test_loss': 0.0
    }
    
    for epoch in range(NUM_EPOCHS):
        train_loss = train_epoch(model, train_loader, optimizer, criterion, 
                                TEACHER_FORCING_RATIO, is_bert)
        val_loss = evaluate(model, val_loader, criterion, is_bert)
        
        results['train_losses'].append(train_loss)
        results['val_losses'].append(val_loss)
        
        print(f'Epoch {epoch+1}/{NUM_EPOCHS} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f}')
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            results['best_epoch'] = epoch + 1
            results['best_val_loss'] = val_loss
            torch.save(model.state_dict(), f'best_model_{config_name}.pt')
            print('  ✓ Model saved!')
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                print(f'  Early stopping at epoch {epoch+1}')
                break
    
    # Load best model and evaluate on test
    model.load_state_dict(torch.load(f'best_model_{config_name}.pt'))
    test_loss = evaluate(model, test_loader, criterion, is_bert)
    results['test_loss'] = test_loss
    
    print(f'\nTest Loss: {test_loss:.4f}')
    
    return results, model


def main():
    """Main comparison experiment"""
    print("="*80)
    print("COMPARACIÓN EXHAUSTIVA DE EMBEDDINGS - SEQ2SEQ SUMMARIZATION")
    print("="*80)
    
    # Load data
    print("\nCargando datos...")
    df = pd.read_csv('data/definitivos/INDEX_ALL_scrapped_filtrado.csv')
    df = df[['article_text', 'headline']].dropna()
    df['article_text'] = df['article_text'].str.lower().str.strip()
    df['headline'] = df['headline'].str.lower().str.strip()
    
    # Filter
    df = df[df['headline'].str.split().str.len() <= MAX_HEADLINE_LEN]
    df = df[df['headline'].str.split().str.len() >= 3]
    
    # Sample for faster experiments (remove for full dataset)
    df = df.sample(min(5000, len(df)), random_state=SEED)
    
    print(f"Total samples: {len(df)}")
    
    # Split
    train_df, temp_df = train_test_split(df, test_size=0.3, random_state=SEED)
    val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=SEED)
    
    print(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")
    
    # Build vocabulary
    print("\nConstruyendo vocabulario...")
    vocab = Vocabulary()
    for _, row in train_df.iterrows():
        vocab.add_sentence(row['article_text'])
        vocab.add_sentence(row['headline'])
    
    print(f"Vocabulary size: {vocab.n_words}")
    
    # Load FastText embeddings
    fasttext_path = 'src/Representacion_del_lenguaje/embeddings/NoContext/fasttext_sg.kv'
    if os.path.exists(fasttext_path):
        embedding_weights = load_fasttext_embeddings(vocab, fasttext_path)
    else:
        print(f"FastText model not found at {fasttext_path}")
        embedding_weights = None
    
    # Create datasets for non-contextual experiments
    train_dataset = SummarizationDataset(
        train_df['article_text'].tolist(), train_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    val_dataset = SummarizationDataset(
        val_df['article_text'].tolist(), val_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    test_dataset = SummarizationDataset(
        test_df['article_text'].tolist(), test_df['headline'].tolist(),
        vocab, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)
    
    all_results = []
    
    # ========================================================================
    # EXPERIMENT 1: FastText Frozen
    # ========================================================================
    if embedding_weights is not None:
        print("\n" + "="*80)
        print("EXPERIMENTO 1: FastText FROZEN (No Contextual)")
        print("="*80)
        
        encoder = EncoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                            embedding_weights, freeze_embeddings=True, 
                            use_pretrained=True).to(device)
        decoder = AttnDecoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                                embedding_weights, freeze_embeddings=True, 
                                use_pretrained=True).to(device)
        model = Seq2Seq(encoder, decoder).to(device)
        
        criterion = nn.CrossEntropyLoss(ignore_index=vocab.word2idx[PAD_TOKEN])
        optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
        
        results, _ = run_experiment("fasttext_frozen", model, train_loader, 
                                   val_loader, test_loader, criterion, optimizer)
        all_results.append(results)
    
    # ========================================================================
    # EXPERIMENT 2: FastText Fine-tuned
    # ========================================================================
    if embedding_weights is not None:
        print("\n" + "="*80)
        print("EXPERIMENTO 2: FastText FINE-TUNED (No Contextual)")
        print("="*80)
        
        encoder = EncoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                            embedding_weights, freeze_embeddings=False, 
                            use_pretrained=True).to(device)
        decoder = AttnDecoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                                embedding_weights, freeze_embeddings=False, 
                                use_pretrained=True).to(device)
        model = Seq2Seq(encoder, decoder).to(device)
        
        criterion = nn.CrossEntropyLoss(ignore_index=vocab.word2idx[PAD_TOKEN])
        optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
        
        results, _ = run_experiment("fasttext_finetuned", model, train_loader, 
                                   val_loader, test_loader, criterion, optimizer)
        all_results.append(results)
    
    # ========================================================================
    # EXPERIMENT 3: Embeddings From Scratch
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENTO 3: Embeddings FROM SCRATCH (Aleatorios)")
    print("="*80)
    
    encoder = EncoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                        embedding_weights=None, freeze_embeddings=False, 
                        use_pretrained=False).to(device)
    decoder = AttnDecoderRNN(vocab.n_words, EMBEDDING_DIM, HIDDEN_SIZE, 
                            embedding_weights=None, freeze_embeddings=False, 
                            use_pretrained=False).to(device)
    model = Seq2Seq(encoder, decoder).to(device)
    
    criterion = nn.CrossEntropyLoss(ignore_index=vocab.word2idx[PAD_TOKEN])
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    
    results, _ = run_experiment("from_scratch", model, train_loader, 
                               val_loader, test_loader, criterion, optimizer)
    all_results.append(results)
    
    # ========================================================================
    # EXPERIMENT 4: BERT Frozen
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENTO 4: BERT FROZEN (Contextual)")
    print("="*80)
    
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    
    train_bert_dataset = BERTSummarizationDataset(
        train_df['article_text'].tolist(), train_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    val_bert_dataset = BERTSummarizationDataset(
        val_df['article_text'].tolist(), val_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    test_bert_dataset = BERTSummarizationDataset(
        test_df['article_text'].tolist(), test_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    train_bert_loader = DataLoader(train_bert_dataset, batch_size=16, shuffle=True)
    val_bert_loader = DataLoader(val_bert_dataset, batch_size=16)
    test_bert_loader = DataLoader(test_bert_dataset, batch_size=16)
    
    encoder = BERTEncoder(HIDDEN_SIZE, freeze_bert=True).to(device)
    decoder = BERTAttnDecoder(tokenizer.vocab_size, HIDDEN_SIZE, freeze_bert=True).to(device)
    model = BERTSeq2Seq(encoder, decoder, tokenizer).to(device)
    
    criterion = nn.CrossEntropyLoss(ignore_index=tokenizer.pad_token_id)
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    
    results, _ = run_experiment("bert_frozen", model, train_bert_loader, 
                               val_bert_loader, test_bert_loader, criterion, 
                               optimizer, is_bert=True)
    all_results.append(results)
    
    # ========================================================================
    # EXPERIMENT 5: BERT Fine-tuned
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENTO 5: BERT FINE-TUNED (Contextual)")
    print("="*80)
    
    encoder = BERTEncoder(HIDDEN_SIZE, freeze_bert=False).to(device)
    decoder = BERTAttnDecoder(tokenizer.vocab_size, HIDDEN_SIZE, freeze_bert=False).to(device)
    model = BERTSeq2Seq(encoder, decoder, tokenizer).to(device)
    
    criterion = nn.CrossEntropyLoss(ignore_index=tokenizer.pad_token_id)
    optimizer = optim.Adam(model.parameters(), lr=1e-5)  # Lower LR for fine-tuning
    
    results, _ = run_experiment("bert_finetuned", model, train_bert_loader, 
                               val_bert_loader, test_bert_loader, criterion, 
                               optimizer, is_bert=True)
    all_results.append(results)
    
    # ========================================================================
    # SUMMARY OF RESULTS
    # ========================================================================
    print("\n" + "="*80)
    print("RESUMEN DE RESULTADOS")
    print("="*80)
    
    print(f"\n{'Configuración':<25} {'Best Epoch':<12} {'Val Loss':<12} {'Test Loss':<12}")
    print("-" * 80)
    
    for result in all_results:
        print(f"{result['config']:<25} {result['best_epoch']:<12} "
              f"{result['best_val_loss']:<12.4f} {result['test_loss']:<12.4f}")
    
    # Save results to JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f'comparison_results_{timestamp}.json'
    
    with open(results_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\nResultados guardados en: {results_file}")
    
    print("\n" + "="*80)
    print("COMPARACIÓN COMPLETADA")
    print("="*80)
    
    print("\n📊 Configuraciones comparadas:")
    print("  1. FastText Frozen (no contextual, congelado)")
    print("  2. FastText Fine-tuned (no contextual, ajustado)")
    print("  3. From Scratch (aleatorio, entrenado desde cero)")
    print("  4. BERT Frozen (contextual, congelado)")
    print("  5. BERT Fine-tuned (contextual, ajustado)")


if __name__ == "__main__":
    main()
