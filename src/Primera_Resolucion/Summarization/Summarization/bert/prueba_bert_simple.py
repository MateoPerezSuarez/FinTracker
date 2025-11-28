"""
Seq2Seq Summarization - Solo BERT
Entrena dos modelos: BERT Frozen y BERT Fine-tuned
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import pandas as pd
import numpy as np
from transformers import BertModel, BertTokenizer
from sklearn.model_selection import train_test_split
import random
from tqdm import tqdm
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
BERT_DIM = 768
BATCH_SIZE = 8  # Smaller for BERT
LEARNING_RATE_FROZEN = 0.001
LEARNING_RATE_FINETUNED = 1e-5  # Much lower for fine-tuning BERT
NUM_EPOCHS = 10
TEACHER_FORCING_RATIO = 0.5
PATIENCE = 3


class BERTSummarizationDataset(Dataset):
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


class BERTEncoder(nn.Module):
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


class BahdanauAttention(nn.Module):
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


class BERTAttnDecoder(nn.Module):
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


class BERTSeq2Seq(nn.Module):
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


def train_epoch(model, dataloader, optimizer, criterion, teacher_forcing_ratio, pad_idx):
    model.train()
    epoch_loss = 0
    
    for batch in tqdm(dataloader, desc="Training"):
        article_ids = batch['article_ids'].to(device)
        article_mask = batch['article_mask'].to(device)
        headline_ids = batch['headline_ids'].to(device)
        
        optimizer.zero_grad()
        output = model(article_ids, article_mask, headline_ids, teacher_forcing_ratio)
        
        output_dim = output.shape[-1]
        output = output[:, 1:].reshape(-1, output_dim)
        headline = headline_ids[:, 1:].reshape(-1)
        
        loss = criterion(output, headline)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        
        epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def evaluate(model, dataloader, criterion, pad_idx):
    model.eval()
    epoch_loss = 0
    
    with torch.no_grad():
        for batch in dataloader:
            article_ids = batch['article_ids'].to(device)
            article_mask = batch['article_mask'].to(device)
            headline_ids = batch['headline_ids'].to(device)
            
            output = model(article_ids, article_mask, headline_ids, 0)
            
            output_dim = output.shape[-1]
            output = output[:, 1:].reshape(-1, output_dim)
            headline = headline_ids[:, 1:].reshape(-1)
            
            loss = criterion(output, headline)
            epoch_loss += loss.item()
    
    return epoch_loss / len(dataloader)


def run_experiment(config_name, model, train_loader, val_loader, test_loader, 
                   criterion, optimizer, pad_idx):
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
                                TEACHER_FORCING_RATIO, pad_idx)
        val_loss = evaluate(model, val_loader, criterion, pad_idx)
        
        results['train_losses'].append(train_loss)
        results['val_losses'].append(val_loss)
        
        print(f'Epoch {epoch+1}/{NUM_EPOCHS} | Train Loss: {train_loss:.4f} | Val Loss: {val_loss:.4f}')
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            results['best_epoch'] = epoch + 1
            results['best_val_loss'] = val_loss
            torch.save(model.state_dict(), f'src/Primera_Resolucion/Summarization/antigravity/best_model_{config_name}.pt')
            print('  ✓ Model saved!')
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                print(f'  Early stopping at epoch {epoch+1}')
                break
    
    # Load best model and evaluate on test
    model.load_state_dict(torch.load(f'src/Primera_Resolucion/Summarization/antigravity/best_model_{config_name}.pt'))
    test_loss = evaluate(model, test_loader, criterion, pad_idx)
    results['test_loss'] = test_loss
    
    print(f'\nTest Loss: {test_loss:.4f}')
    
    return results, model


def main():
    print("="*80)
    print("SEQ2SEQ SUMMARIZATION - BERT EXPERIMENTS")
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
    
    # Sample for faster training
    df = df.sample(min(3000, len(df)), random_state=SEED)
    
    print(f"Total samples: {len(df)}")
    
    # Split
    train_df, temp_df = train_test_split(df, test_size=0.3, random_state=SEED)
    val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=SEED)
    
    print(f"Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")
    
    # Initialize BERT tokenizer
    print("\nInitializing BERT tokenizer...")
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    
    # Create datasets
    train_dataset = BERTSummarizationDataset(
        train_df['article_text'].tolist(), train_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    val_dataset = BERTSummarizationDataset(
        val_df['article_text'].tolist(), val_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    test_dataset = BERTSummarizationDataset(
        test_df['article_text'].tolist(), test_df['headline'].tolist(),
        tokenizer, MAX_ARTICLE_LEN, MAX_HEADLINE_LEN
    )
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)
    test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)
    
    pad_idx = tokenizer.pad_token_id
    criterion = nn.CrossEntropyLoss(ignore_index=pad_idx)
    
    all_results = []
    
    # ========================================================================
    # EXPERIMENT 1: BERT Frozen
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENTO 1: BERT FROZEN")
    print("="*80)
    
    encoder = BERTEncoder(HIDDEN_SIZE, freeze_bert=True).to(device)
    decoder = BERTAttnDecoder(tokenizer.vocab_size, HIDDEN_SIZE, freeze_bert=True).to(device)
    model = BERTSeq2Seq(encoder, decoder, tokenizer).to(device)
    
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE_FROZEN)
    
    results, _ = run_experiment("bert_frozen", model, train_loader, 
                               val_loader, test_loader, criterion, optimizer, pad_idx)
    all_results.append(results)
    
    # ========================================================================
    # EXPERIMENT 2: BERT Fine-tuned
    # ========================================================================
    print("\n" + "="*80)
    print("EXPERIMENTO 2: BERT FINE-TUNED")
    print("="*80)
    
    encoder = BERTEncoder(HIDDEN_SIZE, freeze_bert=False).to(device)
    decoder = BERTAttnDecoder(tokenizer.vocab_size, HIDDEN_SIZE, freeze_bert=False).to(device)
    model = BERTSeq2Seq(encoder, decoder, tokenizer).to(device)
    
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE_FINETUNED)
    
    results, _ = run_experiment("bert_finetuned", model, train_loader, 
                               val_loader, test_loader, criterion, optimizer, pad_idx)
    all_results.append(results)
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*80)
    print("RESUMEN DE RESULTADOS")
    print("="*80)
    
    print(f"\n{'Configuración':<25} {'Best Epoch':<12} {'Val Loss':<12} {'Test Loss':<12}")
    print("-" * 80)
    
    for result in all_results:
        print(f"{result['config']:<25} {result['best_epoch']:<12} "
              f"{result['best_val_loss']:<12.4f} {result['test_loss']:<12.4f}")
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f'bert_comparison_results_{timestamp}.json'
    
    with open(results_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\nResultados guardados en: {results_file}")
    
    print("\n" + "="*80)
    print("EXPERIMENTOS BERT COMPLETADOS")
    print("="*80)
    
    print("\n📊 Modelos entrenados:")
    print("  1. BERT Frozen (contextual, congelado)")
    print("  2. BERT Fine-tuned (contextual, ajustado)")
    print("\n✅ Modelos guardados en: src/Primera_Resolucion/Summarization/antigravity/")


if __name__ == "__main__":
    main()
