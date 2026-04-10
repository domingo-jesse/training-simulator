-- Run this in Supabase SQL Editor to create an app-level user profile table.
-- Includes the exact fields requested for account creation flows.

create extension if not exists pgcrypto;

create table if not exists public.app_users (
    id uuid primary key default gen_random_uuid(),
    email text not null,
    full_name text not null,
    role text not null check (role in ('user', 'admin', 'learner')),
    created_at timestamptz not null default timezone('utc', now())
);

-- Prevent duplicate accounts for the same role + email pair.
create unique index if not exists app_users_email_role_unique_idx
    on public.app_users (lower(email), role);
